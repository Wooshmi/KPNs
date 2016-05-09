let _ =
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

