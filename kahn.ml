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

  type 'a channel =  { in_ch : Pervasives.in_channel ; out_ch : Pervasives.out_channel }
  type 'a in_port = { addr : Unix.sockaddr ; mutable in_ch : Pervasives.in_channel option }
  type 'a out_port = { addr : Unix.sockaddr ; mutable out_ch : Pervasives.out_channel option }

  type 'a query =
    | NewChannel
    | Doco of unit process list
    | Finished of 'a

  let _ = Unix.putenv "SERVER" "FALSE"

  let comport = 42
  let currport = ref 1 lsl 15
  let clientip = '129.199.100.19'
  let servers = ['127.0.0.1']

  let opt_get = function
    | None -> raise Not_Found
    | Some x -> x

  let new_channel () =
    if Unix.getenv "SERVER" = "FALSE" then
        let addr = Unix.(ADDR_INET ((inet_addr_of_string clientip), !currport)) in
        incr currport;
        if Unix.fork () = 0 then begin
          process_port addr;
          exit 0;
        end else
          { addr = addr; in_ch = None }, { addr = addr; out_ch = None }
    else begin
        let fd = Unix.(socket PFINET SOCK_STREAM 0) in
        Unix.(connect fd (ADDR_INET ((inet_addr_of_string clientip), comport)));
        let out_ch = Unix.out_channel_of_descr fd in
        Marshal.to_channel out_ch NewChannel [];
        let in_ch = Unix.in_channel_of_descr fd in
        Marshal.from_channel in_ch
    end

  let rec put v p () =
    if p.out_ch = None then begin
      let fd = Unix.(socket PFINET SOCK_STREAM 0) in
      Unix.connect fd p.addr;
      let out_ch = Unix.out_channel_of_descr fd in
      p.out_ch <- Some out_ch
    end
      Marshal.to_channel (opt_get p.out_ch) v [Clotures];

  let return v () = v

  let rec bind e e' () =
    let v = e () in
    e' v ()

  let run e =
    if Unix.getenv "SERVER" = "FALSE" then
        distribute [e]
    else
        e ()

  let rec get p () =
    if p.in_ch = None then begin
      let fd = Unix.(socket PFINET SOCK_STREAM 0) in
      Unix.connect fd p.addr;
      let in_ch = Unix.in_channel_of_descr fd in
      p.in_ch <- Some in_ch
    end
      Marshal.from_channel (opt_get p.in_ch);

  let doco l () =
    if Unix.getenv "SERVER" = "FALSE" then
        distribute l
    else begin
      let fd = Unix.(socket PFINET SOCK_STREAM 0) in
      Unix.(connect fd (ADDR_INET ((inet_addr_of_string clientip), comport)));
      let out_ch = Unix.out_channel_of_descr fd in
      Marshal.to_channel out_ch (Doco l) [Closures];
      let in_ch = Unix.in_channel_of_descr fd in
      let _ = Marshal.from_channel in_ch in ()
    end


end
