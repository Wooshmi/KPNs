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

  let rec scheduler () =
    if !sch_state then (* No more than one scheduler "running" at a time *)
      ()
    else begin
      sch_state := true;
      while not (Queue.is_empty proc_q) do
        let f = Queue.pop proc_q in f ()
      done;
      sch_state := false
    end

  let new_channel () =
    let c = Queue.create () in
    c, c

  let rec put v c f =
    Queue.push v c;
    Queue.push (fun () -> let _ = f () in ()) proc_q;
    scheduler ()

  let return v f =
    Queue.push (fun () -> let _ = f v in ()) proc_q;
    scheduler ()

  let rec bind e e' f =
    e (fun f' -> Queue.push (fun () -> let _ = (e' f') f in ()) proc_q; scheduler ())

  let run e =
    let res = ref None in
    bind e (fun x -> res := Some x; (fun f -> f ())) (fun () -> ());
    match !res with
    | None -> assert false
    | Some x -> x

  let rec get c f =
    if not (Queue.is_empty c) then
      let v = Queue.pop c in Queue.push (fun () -> let _ = f v in ()) proc_q
    else 
      Queue.push (fun () -> let _ = get c f in ()) proc_q;
    scheduler ()

  let doco l f =
    List.iter (fun p -> Queue.add (fun () -> let _ = p f in ()) proc_q) l;
    scheduler ()

  let run_main = run
end

module NetTh: S = struct
  type 'a process = unit -> 'a
  
  type 'a channel = int
  type 'a in_port = int
  type 'a out_port = int

  let queryPort = 1042
  let communicationPort = 1043
  let mainIP = "192.168.43.146"
  let serversIPs = [|"192.168.43.97"|]
  
  let queryConnectionToMain = ref None (* Used for queries and state reports *)
  let communicationConnectionToMain = ref None (* Used for communication through channels *)

  let get_option x = 
    match x with
    | Some a -> a
    | _ -> assert false
  
  type query =
    | NewChannel
    | Doco of string list
    | Finished of int * string

  type 'a communication =
    | Put of 'a out_port * 'a
    | Get of 'a in_port

  type order = string
  
  let reinit_sockets () =
    let fd1 = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd1 (ADDR_INET (inet_addr_of_string mainIP, queryPort)));
    let in1, out1 = Unix.in_channel_of_descr fd1, Unix.out_channel_of_descr fd1 in
    queryConnectionToMain := Some (fd1, in1, out1);
    let fd2 = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd2 (ADDR_INET (inet_addr_of_string mainIP, communicationPort)));
    let in2, out2 = Unix.in_channel_of_descr fd2, Unix.out_channel_of_descr fd2 in
    communicationConnectionToMain := Some (fd2, in2, out2)

  let close_sockets () =
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
      let _, _, outChannel = get_option !queryConnectionToMain in
      Marshal.to_channel outChannel (Finished (pid, Marshal.to_string v [])) [];
      flush outChannel;
      close_sockets ();
      exit 0
    end
  
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
    let queryConnectionToMainA = !queryConnectionToMain in
    let communicationConnectionToMainA = !communicationConnectionToMain in
    queryConnectionToMain := None;
    communicationConnectionToMain := None;
    Marshal.to_channel outChannel 
      (Doco (List.map (fun x -> Marshal.(to_string (execute_process x) [Closures])) l))
      [];
    flush outChannel;
    queryConnectionToMain := queryConnectionToMainA;
    communicationConnectionToMain := communicationConnectionToMainA;
    let _ = (Marshal.from_channel inChannel : unit) in ()

  let return v () = v
  
  let run e = e ()

  let bind e e' () =
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
      let mf = (Marshal.from_channel inChannel : order) in
      let id = (Marshal.from_channel inChannel : int) in
      let f = (Marshal.from_string mf 0 : int -> unit) in
      f id
    done

  let main f = 
    let myIP = get_ip () in
    
    (* Non-blocking read *)
    let non_blocking_read (_, inChannel, _) = begin
      let headerSize = Marshal.header_size in
      let header = Bytes.create headerSize in
      let headerPos = ref 0 in
      while !headerPos <> headerSize do
        headerPos := !headerPos + input inChannel header (!headerPos) (headerSize - !headerPos);
        Thread.yield ()
      done;
      let dataSize = Marshal.data_size header 0 in
      let data = Bytes.create dataSize in
      let dataPos = ref 0 in
      while !dataPos <> dataSize do
        dataPos := !dataPos + input inChannel data (!dataPos) (dataSize - !dataPos);
        Thread.yield ()
      done;
      Bytes.cat header data
    end in

    (* Processes' states *)
    let pid = ref 0 in
    let pLock = Mutex.create () in
    let pidStatus = Hashtbl.create 1000 in
    let pSLock = Mutex.create () in
   
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
          let v = Queue.pop channel.(id) in
          output_string outChannel v;
          flush outChannel;
          Mutex.unlock wOCLock.(id);
          Mutex.unlock cLock.(id);
          true
        end
      end
    end in
    let read_and_process_communication (fd, inChannel, outChannel) =
      try
        let jokers = ref 50 in
        while true do
          let headerSize = Marshal.header_size in
          let header = Bytes.create headerSize in
          let headerPos = ref 0 in
          while !headerPos <> headerSize do
            headerPos := !headerPos + input inChannel header (!headerPos) (headerSize - !headerPos);
            if !jokers = 0 then
              Thread.yield ()
            else
              decr jokers
          done;
          let dataSize = Marshal.data_size header 0 in
          let data = Bytes.create dataSize in
          let dataPos = ref 0 in
          while !dataPos <> dataSize do
            dataPos := !dataPos + input inChannel data (!dataPos) (dataSize - !dataPos);
            if !jokers = 0 then
              Thread.yield ()
            else
              decr jokers
          done;
          let buf = Bytes.cat header data in
          match (Marshal.from_bytes buf 0 : 'a communication) with
          | Put (id, x) ->
            Mutex.lock cLock.(id);
            Queue.push (Marshal.to_string x []) channel.(id);
            Mutex.unlock cLock.(id);
            let _ = try_pop id in Thread.yield ()
          | Get id -> begin
            Mutex.lock wOCLock.(id);
            Queue.push (fd, inChannel, outChannel) waitingOnChannel.(id);
            Mutex.unlock wOCLock.(id);
            if try_pop id then
              jokers := !jokers + 50
            else
              ()
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
          Marshal.to_channel outChannel x [];
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
      flush outChannel (* Signal that the doco has finished *)
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

    (* Processing of queries and state reports *)
    let read_and_process_queries (fd, inChannel, outChannel) =
      try
        while true do
          let buf = non_blocking_read (fd, inChannel, outChannel) in
          match (Marshal.from_bytes buf 0 : query) with
          | NewChannel ->
            new_channel_main (fd, inChannel, outChannel)
          | Doco l ->
            doco_main ((fd, inChannel, outChannel), l);
          | Finished (id, v) -> begin
            Mutex.lock pSLock;
            Hashtbl.add pidStatus id v;
            Mutex.unlock pSLock;
          end;
          Thread.yield ()
        done
      with | End_of_file -> Unix.close fd; close_in_noerr inChannel; close_out_noerr outChannel
    in
    let main_queries () = begin
      Array.iter (fun x -> let _ = Thread.create read_and_process_queries x in ()) servers;
      let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
      Unix.(bind fd (ADDR_INET (inet_addr_of_string myIP, queryPort)));
      Unix.listen fd 100;
      if Unix.fork () = 0 then
        let _ = Thread.create assign f in
        while true do
          let fd', _ = Unix.accept fd in
          let inChannel, outChannel = Unix.in_channel_of_descr fd', Unix.out_channel_of_descr fd' in
          let _ = Thread.create read_and_process_queries (fd', inChannel, outChannel) in ()
        done
    end in 
    main_queries ();
    let _ = Unix.wait () in
    let _ = Unix.wait () in
    exit 0

  let run_main e =
    if get_ip () = mainIP then
      main Marshal.(to_string (execute_process e) [Closures])
    else begin
      server ();
      exit 0
    end
end

