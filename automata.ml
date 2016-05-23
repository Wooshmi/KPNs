module Automata (K : Kahn.S) = struct
  module K = K
  module Lib = Kahn.Lib(K)
  module Int = struct
    type t = int
    let compare = Pervasives.compare
  end
  module Char = struct
    type t = char
    let compare = Pervasives.compare
  end
  module ISet = Set.Make(Int)
  module CMap = Map.Make(Char)
  open Lib

  type automata = {
    transitions : ISet.t CMap.t array;
    start : ISet.t;
    final : ISet.t;
  }

  let read_automata () =
    let fl = Str.(split_delim (regexp " ") (read_line())) in
    let n = int_of_string (List.hd fl) in
    let m = int_of_string (List.hd (List.tl fl)) in
    let start = ISet.of_list (List.map int_of_string (Str.(split_delim (regexp " ") (read_line())))) in
    let final = ISet.of_list (List.map int_of_string (Str.(split_delim (regexp " ") (read_line())))) in
    let transitions = Array.make n CMap.empty in
    for tr = 1 to m do
      let l = Str.(split_delim (regexp " ") (read_line())) in
      let init = int_of_string (List.hd l) in
      let fin = int_of_string (List.hd (List.tl l)) in
      let label = (List.hd (List.tl (List.tl l))).[0] in
      transitions.(init) <- CMap.add label (ISet.add fin (
        try
          CMap.find label transitions.(init)
        with Not_found -> ISet.empty)) transitions.(init)
    done;
    {
        transitions = transitions;
        start = start;
        final = final;
    }

  let a = read_automata ()
  let word = read_line ()

  let explore (qo : bool K.out_port) : unit K.process =
    let rec aux s pos = begin
      if ISet.mem s a.final && pos = String.length word then
        K.put true qo
      else if pos = String.length word then
        K.doco []
      else(
        try
            let to_explore = CMap.find word.[pos] a.transitions.(s) in
            let to_do = ISet.fold (fun x ls -> x::ls) to_explore [] in
            K.doco (List.map (fun x -> aux x (pos + 1)) to_do)
        with Not_found -> K.doco []
      )
    end
    in
    let to_do = ISet.fold (fun x ls -> x::ls) a.start [] in
    (K.doco (List.map (fun x -> aux x 0) to_do)) >>= (fun _ -> K.put false qo)

  let output (qi : bool K.in_port) : unit K.process =
    (K.get qi) >>= 
      (fun b -> 
        if b then
          Format.printf "YES\n"
        else
          Format.printf "NO\n";
        K.doco [])

  let main : unit K.process =
    (delay K.new_channel ()) >>=
    (fun (q_in, q_out) -> K.doco [explore q_out; output q_in])

end

module E = Automata(Kahn.Th)

let () = E.K.run_main E.main
