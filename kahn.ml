module type S = sig
  type 'a process
  type 'a in_port
  type 'a out_port

  val new_channel: unit -> 'a in_port * 'a out_port
  val put: 'a -> 'a out_port -> unit process
  val get: 'a in_port -> 'a process

  val doco: unit process list -> unit process

  val return: 'a -> 'a process
  val bind: 'a process -> ('a -> 'b process) -> 'b process

  val run: 'a process -> 'a
  val run_main: 'a process -> 'a
end

module Lib (K : S) = struct

  let ( >>= ) x f = K.bind x f

  let delay f x =
    K.bind (K.return ()) (fun () -> K.return (f x))

  let par_map f l =
    let rec build_workers l (ports, workers) =
      match l with
      | [] -> (ports, workers)
      | x :: l ->
          let qi, qo = K.new_channel () in
          build_workers
            l
            (qi :: ports,
             ((delay f x) >>= (fun v -> K.put v qo)) :: workers)
    in
    let ports, workers = build_workers l ([], []) in
    let rec collect l acc qo =
      match l with
      | [] -> K.put acc qo
      | qi :: l -> (K.get qi) >>= (fun v -> collect l (v :: acc) qo)
    in
    let qi, qo = K.new_channel () in
    K.run
      ((K.doco ((collect ports [] qo) :: workers)) >>= (fun _ -> K.get qi))
end


module Th: S = struct
  type 'a process = (unit -> 'a)

  type 'a channel = { q: 'a Queue.t ; m: Mutex.t; }
  type 'a in_port = 'a channel
  type 'a out_port = 'a channel

  let new_channel () =
    let c = {q = Queue.create (); m = Mutex.create ()} in
    c, c

  let put v c () =
    Mutex.lock c.m;
    Queue.push v c.q;
    Mutex.unlock c.m;
    Thread.yield ()

  let rec get c () =
    try
      Mutex.lock c.m;
      let v = Queue.pop c.q in
      Mutex.unlock c.m;
      v
    with Queue.Empty ->
      Mutex.unlock c.m;
      Thread.yield ();
      get c ()

  let doco l () =
    let ths = List.map (fun f -> Thread.create f ()) l in
    List.iter (fun th -> Thread.join th) ths

  let return v () = v

  let bind e e' () =
    let v = e () in
    Thread.yield ();
    e' v ()

  let run e = e ()

  let run_main = run
end

module Pipes: S = struct
  type 'a process = (unit -> 'a)

  type 'a channel = { in_ch : Pervasives.in_channel ; out_ch : Pervasives.out_channel }
  type 'a in_port = 'a channel
  type 'a out_port = 'a channel

  let new_channel () =
    let in_fd, out_fd = Unix.pipe () in
    let q = { in_ch = Unix.in_channel_of_descr in_fd; out_ch = Unix.out_channel_of_descr out_fd } in
    q, q

  let put v c () =
    Marshal.to_channel c.out_ch v [Marshal.Closures]

  let rec get c () =
    Marshal.from_channel c.in_ch

  let doco l () =
    let ths = List.map
      (fun f ->
        let pid = Unix.fork () in
        if pid = 0 then (
          f ();
          exit 0;
        );
        pid) l in
    List.iter (fun pid -> let _ = Unix.waitpid [] pid in ()) ths

  let return v () = v

  let bind e e' () =
    let v = e () in
    e' v ()

  let run e = e ()

  let run_main = run
end

module Seq: S = struct
  type 'a process = (('a -> unit) -> unit)

  type 'a channel = 'a Queue.t
  type 'a in_port = 'a channel
  type 'a out_port = 'a channel

  (* Queue for the processes *)
  let proc_q = Queue.create ()
  let sch_state = ref false

  (* Not the most elegant solution *)
  let morph v = Obj.magic v (* Let me show you a magic trick... *)

  let rec scheduler () =
    if !sch_state then (* No more than one scheduler "running" at a time *)
      ()
    else begin
      sch_state := true;
      while not (Queue.is_empty proc_q) do
        let p, param = Queue.pop proc_q in p param
      done;
      sch_state := false
    end

  let new_channel () =
    let c = Queue.create () in
    c, c

  let rec put v c f =
    Queue.push v c;
    Queue.push (morph f, morph ()) proc_q;
    scheduler ()

  let return v f =
    Queue.push (morph f, morph v) proc_q;
    scheduler ()

  let rec bind e e' f =
    e (fun f' -> Queue.push (morph (e' f'), morph f) proc_q; scheduler ())

  let run e =
    let res = ref None in
    bind e (fun x -> res := Some x; (fun f -> f ())) (fun () -> ());
    match !res with
    | None -> assert false
    | Some x -> x

  let rec get c f =
    if not (Queue.is_empty c) then
      let v = Queue.pop c in Queue.push (morph f, morph v) proc_q
    else Queue.push (morph (get c), morph f) proc_q;
    scheduler ()

  let doco l f =
    List.iter (fun p -> Queue.add (morph p, morph ()) proc_q) l;
    scheduler ()

  let run_main = run
end

module Net: S = struct
  type 'a process = unit -> 'a

  type 'a channel = unit
  type 'a in_port = int
  type 'a out_port = int
  
  type 'a query = 
    | NewChannel
    | Doco of unit process list
    | Finished of int * 'a

  type 'a comm = 
    | Put of int * 'a
    | Get of int

  let queryport = 1042
  let commport = 1043
  let currch = ref 0
  let clientip = "192.168.0.100"
  let servers = Array.map 
    (fun x -> Unix.(ADDR_INET (inet_addr_of_string x, queryport)))
    [|"192.168.0.102"|]
  let currsrv = ref 0
  let channel = Array.make (1 lsl 15) (Queue.create ())
  let waiting_on_channel = Array.make (1 lsl 15) (Queue.create ())
  let docopid = ref 0
  let docopid_status = Hashtbl.create 1000
  let children_pids = Queue.create ()

  let new_channel_main () =
    let ch = !currch in
    incr currch;
    ch, ch

  let new_channel () = 
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, queryport)));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.to_channel out_ch (NewChannel) [];
    flush out_ch;
    let in_ch = Unix.in_channel_of_descr fd in
    let aux = Marshal.from_channel in_ch in
    Unix.close fd;
    close_out_noerr out_ch;
    close_in_noerr in_ch;
    aux
  
  let execute docopid x () = 
    if Unix.fork () = 0 then begin
      let v = x () in
      let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
      Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, queryport)));
      let out_ch = Unix.out_channel_of_descr fd in
      Marshal.to_channel out_ch (Finished (docopid, v)) [];
      flush out_ch;
      Unix.close fd;
      close_out_noerr out_ch;
      exit 0
    end

  let distribute l =
    let docopids = Queue.create () in
    List.iteri 
      (fun pos x ->
        let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
        Unix.(connect fd servers.(!currsrv));
        let out_ch = Unix.out_channel_of_descr fd in
        Marshal.(to_channel out_ch (execute !docopid x) [Closures]);
        flush out_ch;
        Unix.close fd;
        close_out_noerr out_ch;
        Queue.push !docopid docopids;
        incr docopid;
        incr currsrv;
        if !currsrv = Array.length servers then
          currsrv := 0;)
      l;
    while not (Queue.is_empty docopids) do
      let x = Queue.pop docopids in
      if Hashtbl.mem docopid_status x then
        ()
      else
        Queue.push x docopids
    done

  let assign x =
    let currdocopid = !docopid in
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd servers.(!currsrv));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.(to_channel out_ch (execute !docopid x) [Closures]);
    flush out_ch;
    Unix.close fd;
    close_out_noerr out_ch;
    incr docopid;
    incr currsrv;
    if !currsrv = Array.length servers then
      currsrv := 0;
    while not (Hashtbl.mem docopid_status currdocopid) do
      ()
    done;
    Marshal.from_string (Hashtbl.find docopid_status currdocopid) 0

  let kill_all_children () =
    Queue.iter (fun pid -> Unix.kill pid 9) children_pids;
    Queue.clear children_pids

  let rec put v p () =
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, commport)));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.(to_channel out_ch (Put (p, v)) []);
    flush out_ch;
    Unix.close fd;
    close_out_noerr out_ch
  
  let return v () = v

  let bind e e' () =
    let v = e () in
    e' v ()

  let rec get p () =
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, commport)));
    let in_ch = Unix.in_channel_of_descr fd in
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.(to_channel out_ch (Get p) []);
    flush out_ch;
    let aux = Marshal.from_channel in_ch in
    Unix.close fd;
    close_out_noerr out_ch;
    close_in_noerr in_ch;
    aux
  
  let doco_main l () =
    distribute l

  let doco l () =
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, queryport)));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.(to_channel out_ch (Doco l) [Closures]);
    flush out_ch;
    let in_ch = Unix.in_channel_of_descr fd in
    let _ = Marshal.from_channel in_ch in
    Unix.close fd;
    close_out_noerr out_ch;
    close_in_noerr in_ch

  let read_and_execute_query fd =
    let in_ch = Unix.in_channel_of_descr fd in
    let out_ch = Unix.out_channel_of_descr fd in
    let v = Marshal.from_channel in_ch in
    match v with
    | NewChannel -> 
      Marshal.(to_channel out_ch (new_channel_main ()) [Closures]);
      flush out_ch;
      Unix.close fd;
      close_out_noerr out_ch;
      close_in_noerr in_ch
    | Doco l -> 
      doco_main l (); 
      Marshal.(to_channel out_ch () [Closures]); (* doco finish signal *)
      flush out_ch;
      Unix.close fd;
      close_out_noerr out_ch;
      close_in_noerr in_ch
    | Finished (id, x) -> 
      Unix.close fd;
      close_out_noerr out_ch;
      close_in_noerr in_ch;
      Hashtbl.add docopid_status id (Marshal.to_string x [])

  let init_client_query () = 
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(bind fd (ADDR_INET (inet_addr_any, queryport)));
    Unix.(listen fd 100);
    if Unix.fork () = 0 then
      while true do
        let fd', saddr = Unix.accept fd in
        if Unix.fork () = 0 then begin
          read_and_execute_query fd';
          exit 0
        end
      done

  let try_pop id = 
    if Queue.is_empty waiting_on_channel.(id) then
      false
    else begin
      if Queue.is_empty channel.(id) then
        false
      else begin
        let (fd, in_ch, out_ch) = Queue.pop waiting_on_channel.(id) in
        output_string out_ch (Queue.pop channel.(id));
        flush out_ch;
        Unix.close fd;
        close_out_noerr out_ch;
        close_in_noerr in_ch;
        true
      end
    end

  let init_client_comm () =
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(bind fd (ADDR_INET (inet_addr_any, commport)));
    Unix.(listen fd 100);
    if Unix.fork () = 0 then
      while true do
        let fd', saddr = Unix.accept fd in
        let in_ch = Unix.in_channel_of_descr fd' in
        let v = Marshal.from_channel in_ch in
        match v with
        | Put (id, x) ->
          Queue.push Marshal.(to_string x []) channel.(id);
          let _ = try_pop id in 
          Unix.close fd';
          close_in_noerr in_ch
        | Get id ->
          if try_pop id then
            ()
          else
            Queue.push (fd', in_ch, Unix.out_channel_of_descr fd') waiting_on_channel.(id)
      done

  let init_client () =
    Unix.putenv "SERVER" "FALSE";
    init_client_query ();
    init_client_comm ()

  let init_server () = 
    Unix.putenv "SERVER" "TRUE";
    let queryport = 1042 in
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(bind fd (ADDR_INET (inet_addr_any, queryport)));
    Unix.listen fd 100;
    while true do
      let fd', _ = Unix.accept fd in
      let in_ch = Unix.in_channel_of_descr fd' in
      let (f : unit -> 'a) = Marshal.from_channel in_ch in
      f ();
      Unix.close fd';
      close_in_noerr in_ch;
    done

  let run e =
    try
      let _ = Unix.getenv "INIT" in
      if Unix.getenv "SERVER" = "FALSE" then begin
        let v = assign e in
        kill_all_children ();
        v
      end else 
        e ()
    with 
    | Not_found -> begin
      Unix.putenv "INIT" "42";
      let ip = Unix.(string_of_inet_addr ((gethostbyname (gethostname () ^ ".local")).h_addr_list.(0))) in
      if ip = clientip then begin
        init_client ();
        let v = assign e in
        kill_all_children ();
        v
      end else begin
        init_server ();
        exit 0
      end
    end

  let run_main = run
end

module NetTh: S = struct
  type 'a process = unit -> 'a
  
  type 'a channel = int
  type 'a in_port = int
  type 'a out_port = int

  let statePort = 1041
  let queryPort = 1042
  let communicationPort = 1043
  let mainIP = "192.168.0.100"
  let serversIPs = [|"192.168.0.102"|]
  
  let stateConnectionToMain = ref None (* Used for finish reports *)
  let queryConnectionToMain = ref None (* Used for queries *)
  let communicationConnectionToMain = ref None (* Used for communication through channels *)

  let get_option x = 
    match x with
    | Some a -> a
    | _ -> assert false
  
  type query =
    | NewChannel
    | Doco of string list
    | Bind of string * string

  type 'a communication =
    | Put of int * 'a
    | Get of int

  type order =
    | Process of string
    | Binder of string * string
  
  let reinit_sockets () =
    let fd1 = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd1 (ADDR_INET (inet_addr_of_string mainIP, statePort)));
    let in1, out1 = Unix.in_channel_of_descr fd1, Unix.out_channel_of_descr fd1 in
    stateConnectionToMain := Some (fd1, in1, out1);
    let fd2 = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd2 (ADDR_INET (inet_addr_of_string mainIP, queryPort)));
    let in2, out2 = Unix.in_channel_of_descr fd2, Unix.out_channel_of_descr fd2 in
    queryConnectionToMain := Some (fd2, in2, out2);
    let fd3 = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd3 (ADDR_INET (inet_addr_of_string mainIP, communicationPort)));
    let in3, out3 = Unix.in_channel_of_descr fd3, Unix.out_channel_of_descr fd3 in
    communicationConnectionToMain := Some (fd3, in3, out3)

  let close_sockets () =
    let fd, inChannel, outChannel = get_option !stateConnectionToMain in
    Unix.close fd;
    close_in_noerr inChannel;
    close_out_noerr outChannel;
    let fd, inChannel, outChannel = get_option !queryConnectionToMain in
    Unix.close fd;
    close_in_noerr inChannel;
    close_out_noerr outChannel;
    let fd, inChannel, outChannel = get_option !communicationConnectionToMain in
    Unix.close fd;
    close_in_noerr inChannel;
    close_out_noerr outChannel

  let execute_process x pid =
    if Unix.fork () = 0 then begin
      reinit_sockets ();
      let v = x () in
      let _, _, outChannel = get_option !stateConnectionToMain in
      Marshal.to_channel outChannel (pid, v) [];
      flush outChannel;
      close_sockets ();
      exit 0
    end
  
  let execute_binder x param = execute_process (x param)
  
  let new_channel () =
    let _, inChannel, outChannel = get_option !queryConnectionToMain in
    Marshal.to_channel outChannel 
      NewChannel
      [];
    flush outChannel;
    let aux = (Marshal.from_channel inChannel : 'a in_port * 'a out_port) in
    aux

  let put v p () =
    let _, _, outChannel = get_option !communicationConnectionToMain in
    Marshal.to_channel outChannel 
      (Put (p, v))
      [];
    flush outChannel
  
  let get p () =
    let _, inChannel, outChannel = get_option !communicationConnectionToMain in
    Marshal.to_channel outChannel 
      (Get p) 
      [];
    flush outChannel;
    Marshal.from_channel inChannel

  let doco l () =
    let _, inChannel, outChannel = get_option !queryConnectionToMain in
    let stateConnectionToMainA = !stateConnectionToMain in
    let queryConnectionToMainA = !queryConnectionToMain in
    let communicationConnectionToMainA = !communicationConnectionToMain in
    stateConnectionToMain := None;
    queryConnectionToMain := None;
    communicationConnectionToMain := None;
    Marshal.to_channel outChannel 
      (Doco (List.map (fun x -> Marshal.(to_string (execute_process x) [Closures])) l))
      [];
    flush outChannel;
    stateConnectionToMain := stateConnectionToMainA;
    queryConnectionToMain := queryConnectionToMainA;
    communicationConnectionToMain := communicationConnectionToMainA;
    let _ = (Marshal.from_channel inChannel : unit) in ()

  let return v () = v
  
  let run e = e ()

  let bind e e' () =
    (*let _, inChannel, outChannel = get_option !queryConnectionToMain in
    let stateConnectionToMainA = !stateConnectionToMain in
    let queryConnectionToMainA = !queryConnectionToMain in
    let communicationConnectionToMainA = !communicationConnectionToMain in
    stateConnectionToMain := None;
    queryConnectionToMain := None;
    communicationConnectionToMain := None;
    Marshal.to_channel outChannel 
      (Bind 
        (Marshal.(to_string (execute_process e) [Closures]), 
        Marshal.(to_string (execute_binder e') [Closures]))
      )
      [];
    flush outChannel;
    stateConnectionToMain := stateConnectionToMainA;
    queryConnectionToMain := queryConnectionToMainA;
    communicationConnectionToMain := communicationConnectionToMainA;
    Marshal.from_channel inChannel *) (* Network bind not working yet. *)
    e' (e ()) ()
  
  let get_ip () = 
    Unix.(string_of_inet_addr (gethostbyname (Unix.gethostname () ^ ".local")).h_addr_list.(0))
  
  let server () = 
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    let myIP = get_ip () in
    Unix.(bind fd (ADDR_INET (inet_addr_of_string myIP, queryPort)));
    Unix.listen fd 100;
    let fd', _ = Unix.accept fd in
    let inChannel, outChannel = Unix.in_channel_of_descr fd', Unix.out_channel_of_descr fd' in
    while true do
      match Marshal.from_channel inChannel with
      | Process mf ->
        let id = Marshal.from_channel inChannel in
        let f = (Marshal.from_string mf 0 : int -> unit) in
        f id
      | Binder (mf, mp) ->
        let id = Marshal.from_channel inChannel in
        let f = (Marshal.from_string mf 0 : 'a -> int -> unit) in
        let p = (Marshal.from_string mp 0 : 'a) in
        f p id
    done

  let main f = 
    let myIP = get_ip () in

    (* Processing of processes' states *)
    let pid = ref 0 in
    let pLock = Mutex.create () in
    let pidStatus = Hashtbl.create 1000 in
    let pSLock = Mutex.create () in
    let read_and_process_state (fd, inChannel, outChannel) = 
      try
        while true do 
          Thread.yield ();
          let id, x = (Marshal.from_channel inChannel : int * 'a) in
          Mutex.lock pSLock;
          Hashtbl.add pidStatus id (Marshal.to_string x []);
          Mutex.unlock pSLock;
        done
      with | End_of_file -> Unix.close fd; close_in_noerr inChannel; close_out_noerr outChannel
    in
    let main_state () = begin
      let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
      Unix.(bind fd (ADDR_INET (inet_addr_of_string myIP, statePort)));
      Unix.listen fd 100;
      if Unix.fork () = 0 then
        while true do
          let fd', _ = Unix.accept fd in
          let inChannel, outChannel = Unix.in_channel_of_descr fd', Unix.out_channel_of_descr fd' in
          let _ = Thread.create read_and_process_state (fd', inChannel, outChannel) in ()
        done
    end in main_state ();
    
    (* Processing of communication through channels *)
    let currentChannel = ref 0 in
    let cCLock = Mutex.create () in
    let maxChannels = 1 lsl 10 in
    let channel = Array.make maxChannels (Queue.create ()) in
    let cLock = Array.make maxChannels (Mutex.create ()) in
    let waitingOnChannel = Array.make maxChannels (Queue.create ()) in
    let wOCLock = Array.make maxChannels (Mutex.create ()) in
    let try_pop id = begin
      Mutex.lock wOCLock.(id);
      if Queue.is_empty waitingOnChannel.(id) then begin
        Mutex.unlock wOCLock.(id);
        false
      end else begin
        Mutex.lock cLock.(id);
        if Queue.is_empty channel.(id) then begin
          Mutex.unlock wOCLock.(id);
          Mutex.unlock cLock.(id);
          false
        end else begin
          let (_, inChannel, outChannel) = Queue.pop waitingOnChannel.(id) in
          output_string outChannel (Queue.pop channel.(id));
          flush outChannel;
          Mutex.unlock wOCLock.(id);
          Mutex.unlock cLock.(id);
          true
        end
      end
    end in
    let read_and_process_communication (fd, inChannel, outChannel) =
      try
        while true do
          Thread.yield ();
          match (Marshal.from_channel inChannel: 'a communication) with
          | Put (id, x) ->
            Mutex.lock cLock.(id);
            Queue.push (Marshal.to_string x []) channel.(id);
            Mutex.unlock cLock.(id);
            let _ = try_pop id in ()
          | Get id ->
            if try_pop id then
              ()
            else begin
              Mutex.lock wOCLock.(id);
              Queue.push (fd, inChannel, outChannel) waitingOnChannel.(id);
              Mutex.unlock wOCLock.(id)
            end
        done
      with | End_of_file -> Unix.close fd; close_in_noerr inChannel; close_out_noerr outChannel
    in
    let main_communication () = begin
      let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
      Unix.(bind fd (ADDR_INET (inet_addr_of_string myIP, communicationPort)));
      Unix.listen fd 100;
      if Unix.fork () = 0 then
        while true do
          let fd', _ = Unix.accept fd in
          let inChannel, outChannel = Unix.in_channel_of_descr fd', Unix.out_channel_of_descr fd' in
          let _ = Thread.create read_and_process_communication (fd', inChannel, outChannel) in ()
        done
    end in main_communication ();

    (* Basic function *)
    let currentServer = ref 0 in
    let cSLock = Mutex.create () in
    let servers = Array.map
      (fun ip ->
        let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
        Unix.(connect fd (ADDR_INET (inet_addr_of_string ip, queryPort)));
        fd, Unix.in_channel_of_descr fd, Unix.out_channel_of_descr fd) serversIPs in
    let new_channel_main (_, _, outChannel) = begin
      Mutex.lock cCLock;
      let ch = !currentChannel in
      incr currentChannel;
      Mutex.unlock cCLock;
      Marshal.to_channel outChannel (ch, ch) [];
      flush outChannel
    end in
    let doco_main ((_, _, outChannel), l) = begin
      let children = Queue.create () in
      List.iter
        (fun x ->
          Mutex.lock cSLock;
          Mutex.lock pLock;
          incr pid;
          let _, _, outChannel = servers.(!currentServer) in
          Marshal.to_channel outChannel (Process x) [];
          Marshal.to_channel outChannel !pid [];
          flush outChannel;
          Queue.push !pid children;
          incr currentServer;
          if !currentServer = Array.length servers then
            currentServer := 0;
          Mutex.unlock cSLock;
          Mutex.unlock pLock
        )
        l;
      let rec wait () = begin
        Thread.yield ();
        if Queue.is_empty children then
          ()
        else begin
          let x = Queue.pop children in
          Mutex.lock pSLock;
          if Hashtbl.mem pidStatus x then begin
            Mutex.unlock pSLock;
            wait ()
          end else begin
            Mutex.unlock pSLock;
            Queue.push x children
          end
        end
      end in 
      wait ();
      Marshal.to_channel outChannel () [];
      flush outChannel(* Signal that the doco has finished *)
    end in
    let assign x = begin
      Mutex.lock cSLock;
      Mutex.lock pLock;
      incr pid;
      let currentpid = !pid in
      let _, _, outChannel = servers.(!currentServer) in
      Marshal.to_channel outChannel x [];
      Marshal.to_channel outChannel currentpid [];
      flush outChannel;
      incr currentServer;
      if !currentServer = Array.length servers then
        currentServer := 0;
      Mutex.unlock cSLock;
      Mutex.unlock pLock;
      let rec wait () = begin
        Thread.yield ();
        Mutex.lock pSLock;
        if Hashtbl.mem pidStatus currentpid then begin
          Mutex.unlock pSLock;
          Hashtbl.find pidStatus currentpid
        end else
          wait ()
      end in
      Marshal.from_string (wait ()) 0
    end in
    let bind_main ((_, _, outChannel), (e, e')) = begin
      let v = assign (Process e) in 
      let h = assign (Binder (e', Marshal.to_string v [])) in
      Marshal.to_channel outChannel h [];
      flush outChannel
    end in

    (* Processing of queries *)
    let read_and_process_queries (fd, inChannel, outChannel) =
      try
        while true do
          Thread.yield ();
          match (Marshal.from_channel inChannel : query) with
          | NewChannel ->
            new_channel_main (fd, inChannel, outChannel);
          | Doco l ->
            doco_main ((fd, inChannel, outChannel), l);
          | Bind (e, e') ->
            bind_main ((fd, inChannel, outChannel), (e, e'));
        done
      with | End_of_file -> Unix.close fd; close_in_noerr inChannel; close_out_noerr outChannel
    in
    let main_queries () = begin
      Array.iter (fun x -> let _ = Thread.create read_and_process_queries x in ()) servers;
      let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
      Unix.(bind fd (ADDR_INET (inet_addr_of_string myIP, queryPort)));
      Unix.listen fd 100;
      if Unix.fork () = 0 then
        while true do
          let fd', _ = Unix.accept fd in
          let inChannel, outChannel = Unix.in_channel_of_descr fd', Unix.out_channel_of_descr fd' in
          let _ = Thread.create read_and_process_queries (fd', inChannel, outChannel) in ()
        done
    end in main_queries ();
    let assign_first x = begin
      pid := -1;
      assign x
    end in assign_first (Process f)

  let run_main e =
    if get_ip () = mainIP then
      main Marshal.(to_string (execute_process e) [Closures])
    else begin
      server ();
      exit 0
    end
end

