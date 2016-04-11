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

  type 'a channel = { in_fd : Unix.file_descr ; out_fd : Unix.file_descr }
  type 'a in_port = 'a channel
  type 'a out_port = 'a channel

  let prefixLength = 8

  let bytes_of_int x =
    Bytes.init prefixLength
      (fun i -> Char.chr ((x lsr (8 * (prefixLength - i))) land (1 lsl 8 - 1)))

  let int_of_bytes b =
    let ans = ref 0 in
    for i = prefixLength - 1 downto 0 do
      ans := (!ans lsl 8) + Char.code b.[i]
    done;
    !ans

  let new_channel () =
    let out_fd, in_fd = Unix.pipe () in
    let q = { in_fd = in_fd; out_fd = out_fd } in
    q, q

  let put v c () =
    let v' = Marshal.to_bytes v [Marshal.Closures] in
    let _ = Unix.write c.in_fd (bytes_of_int (Bytes.length v')) 0 prefixLength in
    let _ = Unix.write c.in_fd v' 0 (Bytes.length v') in ()

  let rec get c () =
    try
      let b = Bytes.create prefixLength in
      let _ = Unix.read c.out_fd b 0 prefixLength in
      let length = int_of_bytes b in
      let b' = Bytes.create length in
      let _ = Unix.read c.out_fd b' 0 length in
      Marshal.from_bytes b' 0
    with Unix.Unix_error (Unix.EBADF, _, _) -> get c ()

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

  (* Not the most elegant solution *)
  let hide v = Obj.magic v  (* Let me show you a magic trick... *)

  let rec scheduler () =
    try
      let p, param = Queue.pop proc_q in
      p param;
      scheduler ()
    with Queue.Empty -> ()

  let new_channel () =
    let c = Queue.create () in
    c, c

  let rec put v c f =
    Queue.push v c;
    Queue.push (hide f, hide ()) proc_q;
    scheduler ()

  let return v f =
    Queue.push (hide f, hide v) proc_q;
    scheduler ()

  let rec bind e e' f =
    e (fun f' -> Queue.push (hide (e' f'), hide f) proc_q; scheduler ())

  let run e =
    let res = ref None in
    bind e (fun x -> res := Some x; (fun f -> f ())) (fun () -> ());
    match !res with
    | None -> assert false
    | Some x -> x

  let rec get c f =
    try
      let v = Queue.pop c in
      Queue.push (hide f, hide v) proc_q;
      scheduler ()
    with Queue.Empty ->
      Queue.push (hide (get c), hide f) proc_q;
      scheduler ()

  let doco l f =
    List.iter (fun p -> Queue.add (hide p, hide ()) proc_q) l;
    scheduler ()
end
