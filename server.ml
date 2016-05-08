let _ =
  Unix.putenv "SERVER" "FALSE";
  let comport = 42 in
  let fd = Unix.(socket PF_INET SOCK_STREAM 0) in
  Unix.(bind fd (ADDR_INET (inet_addr_any, comport)));
  Unix.listen fd 100;
  print_endline "Listening...";
  let fd', _ = Unix.accept fd in
  print_endline "Connection from Main Server accepted...";
  let in_ch = Unix.in_channel_of_descr fd' in
  while true do
    print_endline "Waiting for data...";
    let f = Marshal.from_channel in_ch in
    print_endline "Executing...";
    if Unix.fork () = 0 then
      f ()
  done

