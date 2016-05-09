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
end

module Net: S = struct
  type 'a process = unit -> 'a

  type 'a channel = unit
  type 'a in_port = int
  type 'a out_port = int

  let _ = Random.self_init ()
  let comport = 1042
  let currch = ref 0
  (*let currch_lock = Mutex.create ()*)
  let clientip = "192.168.0.102"
  let servers = Array.map 
    (fun x -> Unix.(ADDR_INET (inet_addr_of_string x, comport)))
    [|"192.168.0.100"|]
  let currsrv = ref 0
  (*let currsrv_lock = Mutex.create ()*)
  let channels = Array.make (1 lsl 15) (Queue.create ())
  let docopid = ref 0
  (*let docopid_lock = Mutex.create ()*)
  let docopid_status = Hashtbl.create 1000
  let children_pids = Queue.create ()

  let new_channel_main () =
    (*Mutex.lock currch_lock;*)
    let ch = !currch in
    incr currch;
    (*Mutex.unlock currch_lock;*)
    ch, ch

  let new_channel () = 
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, comport)));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.to_channel out_ch ("NewChannel", ((), ())) [];
    let in_ch = Unix.in_channel_of_descr fd in
    let aux = Marshal.from_channel in_ch in
		Unix.close fd;
		aux
  
  let execute docopid x () = 
    let v = x () in
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, comport)));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.to_channel out_ch ("Finished", (docopid, v)) [];
	  Unix.close fd

  let distribute l =
    let docopids = Queue.create () in
    List.iteri 
      (fun pos x ->
        (*Mutex.lock docopid_lock;
        Mutex.lock currsrv_lock;*)
		    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
		    Unix.(connect fd servers.(!currsrv));
		    let out_ch = Unix.out_channel_of_descr fd in
        Marshal.(to_channel out_ch (execute !docopid x) [Closures]);
        Unix.close fd;
		    Queue.push !docopid docopids;
        incr docopid;
        incr currsrv;
        if !currsrv = Array.length servers then
          currsrv := 0;
        (*Mutex.unlock docopid_lock;
        Mutex.unlock currsrv_lock*))
      l;
    while not (Queue.is_empty docopids) do
      let x = Queue.pop docopids in
      if Hashtbl.mem docopid_status x then
        ()
      else
        Queue.push x docopids
    done

  let assign x =
    (*Mutex.lock docopid_lock;
    Mutex.lock currsrv_lock;*)
    let currdocopid = !docopid in
	  let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
	  Unix.(connect fd servers.(!currsrv));
	  let out_ch = Unix.out_channel_of_descr fd in
	  print_endline "Assign Mashal 1 S";
	  Marshal.(to_channel out_ch (execute !docopid x) [Closures]);
	  print_endline "Assign Mashal 1 E";
	  Unix.close fd;
    incr docopid;
    incr currsrv;
    if !currsrv = Array.length servers then
      currsrv := 0;
    (*Mutex.unlock docopid_lock;
    Mutex.unlock currsrv_lock;*)
    while not (Hashtbl.mem docopid_status currdocopid) do
      ()
    done;
    Marshal.from_string (Hashtbl.find docopid_status currdocopid) 0

  let kill_all_children () =
    Queue.iter (fun pid -> Unix.kill pid 9) children_pids;
    Queue.clear children_pids

  let rec put v p () =
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, comport)));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.(to_channel out_ch ("Put", (p, v)) [Closures]);
    Unix.close fd

  let return v () = v

  let bind e e' () =
    let v = e () in
    e' v ()

  let run e =
    if Unix.getenv "SERVER" = "FALSE" then begin
      let v = assign e in
      kill_all_children ();
      v
    end else 
      e ()

  let rec get p () =
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, comport)));
    let in_ch = Unix.in_channel_of_descr fd in
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.(to_channel out_ch ("Get", (p, ())) []);
    let aux = Marshal.from_channel in_ch in
    Unix.close fd;
    aux
  
  let doco_main l () =
    distribute l

  let doco l () =
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(connect fd (ADDR_INET (inet_addr_of_string clientip, comport)));
    let out_ch = Unix.out_channel_of_descr fd in
    Marshal.(to_channel out_ch ("Doco", (Random.int (1 lsl 30 - 1), l)) [Closures]); (* Docos should be counted *)
    let in_ch = Unix.in_channel_of_descr fd in
    let _ = Marshal.from_channel in_ch in 
	  Unix.close fd

  let read_and_execute fd =
    let in_ch = Unix.in_channel_of_descr fd in
    let out_ch = Unix.out_channel_of_descr fd in
    let (v : string * 'a) = Marshal.from_channel in_ch in
    match fst v with
    | "NewChannel" -> 
      Marshal.(to_channel out_ch (new_channel_main ()) [Closures])
    | "Doco" -> 
      let did, l = snd v in
      if Unix.fork () = 0 then begin
        doco_main l (); 
        Marshal.(to_channel out_ch ("Finished", (did, ())) [Closures])
      end
    | "Finished" -> 
      let id, x = snd v in 
      Hashtbl.add docopid_status id (Marshal.to_string x [])
    | "Put" -> 
      let id, x = snd v in
      Queue.push Marshal.(to_string x [Closures]) channels.(id)
    | "Get" -> 
      let id, _ = snd v in
      let rec aux () =
        try
          Queue.pop channels.(id)
        with
        | Queue.Empty -> aux () in
      Marshal.(to_channel out_ch (Marshal.from_string (aux ())) [Closures])
    | _ -> assert false
  
  let _ = 
    Unix.putenv "SERVER" "FALSE";
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(bind fd (ADDR_INET (inet_addr_any, comport)));
    Unix.(listen fd 100);
    if Unix.fork () = 0 then
      while true do
        let fd', saddr = Unix.accept fd in
        let pid = Unix.fork () in
        if pid = 0 then begin
          read_and_execute fd';
          exit 0;
        end
      done
end
