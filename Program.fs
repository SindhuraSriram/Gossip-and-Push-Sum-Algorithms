open System

open Gossip

[<EntryPoint>]
let main args = 
    let numNodes = if args.Length > 0 then args.[0] |> int else 10 // default value
    let topology = if args.Length > 1 then args.[1] else "full" // default value
    let algo = if args.Length > 2 then args.[2] else "gossip" // default value
    
    setupTopology numNodes topology algo

    System.Console.ReadLine() |> ignore
    0 // Return an integer exit code



