module Example (K : Kahn.S) = struct
  module K = K
  module Lib = Kahn.Lib(K)
  open Lib

  let integers (qo : int K.out_port) : unit K.process =
    let rec loop n =
      (K.put n qo) >>= (fun () -> loop (n + 1))
    in
    loop 2

  let output (qi : int K.in_port) : unit K.process =
    let rec loop () =
      (K.get qi) >>= (fun v -> Format.printf "%d@." v; loop ())
    in
    loop ()

  let main : unit K.process =
    (delay K.new_channel ()) >>=
    (fun (q_in, q_out) -> K.doco [integers q_out; output q_in])

end

module E = Example(Kahn.Net)

let () = 
  if Array.length Sys.argv - 1 = 0 then
    E.K.run E.main
  else begin
    Unix.putenv "SERVER" "TRUE";
    let comport = 1042 in
    let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
    Unix.(bind fd (ADDR_INET (inet_addr_any, comport)));
    Unix.listen fd 100;
    print_endline "Listening...";
    while true do
      let fd', _ = Unix.accept fd in
      print_endline "Connection from Main Server accepted...";
      if Unix.fork () = 0 then begin
        let in_ch = Unix.in_channel_of_descr fd' in
        print_endline "Waiting for data...";
        let f = Marshal.from_channel in_ch in
        print_endline "Executing...";
        f ()
      end
    done
  end


