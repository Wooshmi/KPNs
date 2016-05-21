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

  let g = [|ISet.of_list [1;2;3] ; ISet.of_list [0;3] ; ISet.of_list [0;3] ; ISet.of_list [0;1;2]|]

  let explore (qo : int list K.out_port) : unit K.process =
    let rec aux n s l = begin
      if ISet.cardinal s = Array.length g then
        K.put l qo
      else (
        let to_explore = ISet.diff g.(n) s in
        let to_do = ISet.fold (fun x ls -> x::ls) to_explore [] in
        K.doco (List.map (fun x -> aux x (ISet.add x s) (x::l)) to_do)
      )
    end
    in aux 0 (ISet.singleton 0) [0]


  let output (qi : int list K.in_port) : unit K.process =
    (K.get qi) >>= (fun v -> List.iter (fun x -> Format.printf "%d " x) (List.rev v); K.doco [])

  let main : unit K.process =
    (delay K.new_channel ()) >>=
    (fun (q_in, q_out) -> K.doco [explore q_out; output q_in])

end

module E = Hamilton(Kahn.Th)

let () = E.K.run_main E.main
