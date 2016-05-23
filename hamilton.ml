module Hamilton (K : Kahn.S) = struct
  module K = K
  module Lib = Kahn.Lib(K)
  module Int = struct
    type t = int
    let compare = Pervasives.compare
  end
  module ISet = Set.Make(Int)
  open Lib

  type graph = ISet.t array

  let read_graph () =
    let n = read_int() in
    let g = Array.make n ISet.empty in
    for edge = 0 to n-1 do
      let l = Str.(split_delim (regexp " ") (read_line())) in
      let neighbours = List.map int_of_string l in
      g.(edge) <- ISet.of_list neighbours
    done;
    g

  let g = read_graph ()

  let explore (qo : int list K.out_port) : unit K.process =
    let st = 0 in
    let rec aux n s l = begin
      if ISet.cardinal s = Array.length g && ISet.mem st g.(n) then
        K.put l qo
      else (
        let to_explore = ISet.diff g.(n) s in
        let to_do = ISet.fold (fun x ls -> x::ls) to_explore [] in
        K.doco (List.map (fun x -> aux x (ISet.add x s) (x::l)) to_do)
      )
    end
    in 
    (aux st (ISet.singleton st) [st]) >>= (fun _ -> K.put [] qo)

  let output (qi : int list K.in_port) : unit K.process =
    (K.get qi) >>= 
      (fun v -> 
        if v = [] then
          Format.printf "No Hamiltonian cycle." 
        else
          List.iter (fun x -> Format.printf "%d " x) (List.rev v);
        Format.printf "\n"; 
        K.doco [])

  let main : unit K.process =
    (delay K.new_channel ()) >>=
    (fun (q_in, q_out) -> K.doco [explore q_out; output q_in])

end

module E = Hamilton(Kahn.Th)

let () = E.K.run_main E.main
