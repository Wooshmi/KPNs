# *Compilation and usage*
`make`  
If you want to delete the files created by `make`, you can use:  
`make clean`  
If you want to write your own example `my_example.ml`, inspire yourself from our demo sources (`automata.ml`, `example.ml`, `graph.ml`), and compile it using:  
`ocamlbuild my_example.byte -libs unix -tags thread`  
You can also compile it to native. However, native files won't work when using the network implementation because of Marshaling.  
# *KPNs*
## KPN implementation using Unix Pipes
Unix Pipes are used as channels in this implementation.  
```ocaml
type 'a channel = { 
    in_ch: Pervasives.in_channel; 
    out_ch : Pervasives.out_channel
}
``` 
In a doco we execute each process on its own OS process (with `fork`) and we wait by using `waitpid` on all child processes related to the current doco.
## Sequential KPN implementation
In this implementation we have used a queue for each channel.  
```ocaml
type 'a channel = 'a Queue.t
```  
We have also modified the process type to simulate CPS (continuation-passing style).
```ocaml
type 'a process = ('a -> unit) -> unit
```  
We have a global queue for processes that need to be executed. There's a scheduler that sequentially execute the processes in the queue until the queue becomes empty. We have at most one scheduler running at any given time.  
Our implementation is inspired by "A poor man’s concurrency monad, Koen Classen, 1999". However `run` was implemented differently: we used a reference to save the value returned by the process that was executed.
```ocaml
    let res = ref None in 
    bind e (fun x -> res := Some x; (fun f -> f ()) (fun () -> ())
```
## Network KPN implementation
In order to use this implementation, you'll need at least 2 computers. One of the computers will act as main server, and the others will be workers. *This implementation only works on LAN connections. Make sure that the ports 1042 and 1043 are opened locally.*   
Before compiling, you'll need to modify the module code by filling in the following variables: `mainIP` (line 201) and `serversIPs` (line 202).  
The port 1042 is used for queries. The query type is the following:  
```ocaml
type query = 
| NewChannel (* Ask the main server to create a new channel. *)
| Doco of string list (* Ask the main server to distribute a doco. *)
| Finished of int * string (* Signal the main server the id and return value of a finished process. *)
```
The port 1043 is used for communication. The communication type is the following:
```ocaml
type 'a in_port = int
type 'a out_port = int
type 'a communication =
| Put of 'a out_port * 'a (* Ask the main server to put a value on a specific channel *)
| Get of 'a in_port (* Ask the main server to get a value from a specific channel and send it to you *) 
```  
The data that passes through these 2 ports is completely independent and thus we can have an OS process for each of the ports. On each of these OS processes we will create a thread per incoming connection.  
We had to add one more function to the given signature (`run_main`) which is used for initialisation and should be called only once (it replaces the first run).
### Optimisations
We've implemented a mini priority system for OCaml threads. The issue that motivated us to do this was the fact that `get` commands block the execution of a specific process until it receives the answer. We use jokers to reduce the interruption of communication threads that read a `get`.  All communication processes start with 50 jokers and receive 50 jokers whenever they succesfully read a get request. However they lose 1 joker every time they skip a thread yield. If the number of jokers of a thread reaches 0 then all thread yield are executed.
```ocaml
if !jokers = 0 then
    Thread.yield ()
else
    decr jokers
```  
We've also implemented a lock-free read. Let's suppose that we want to read `size` characters from `inChannel`. We are going to take advantage of the `input` function of the Pervasives module which only reads the available characters.
```ocaml
    let buf = Bytes.create size in
    let pos = ref 0 in
    while pos <> size do
        pos := !pos + input inChannel buf !pos (size - !pos);
        Thread.yield ()
    done
```
# *Examples*
The ideas are simple: whenever we are faced with multiple possibilities we explore all of them with a doco.
## Hamiltonian Cycle Detector
You can execute it on the sample test with the following command:  
`./hamilton.byte < tests/graph.in`  
`./hamilton.byte < tests/graph_neg.in`  
The input format is the following:
- On the first line: `nbNodes` - the number of nodes
- On line n (starting at 0): the neighbours of node (n - 1) separated by spaces  

## Non-deterministic Automata Simulator "2016"
You can execute it on the sample test with the following command:  
`./automata.byte < tests/automata.in`  
`./automata.byte < tests/automata_neg.in`  
The input format is the following:
- On the first line: `nbStates` - the number of states followed by `nbTransitions` - the number of transitions
- On the second line: the starting states separated by spaces
- On the third line: the final states separated by spaces
- On each of the following `nbTransitions` lines: a transition `start end char`
- On the last line: the word that we want to test
