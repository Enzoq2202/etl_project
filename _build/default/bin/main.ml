open Etl_lib.Impure
open Etl_lib.Pure

let () =
  try
    let orders = read_orders "order.csv" in
    let items = read_order_items "order_item.csv" in
    Printf.printf "Loaded %d orders\n" (List.length orders);
    Printf.printf "Loaded %d items\n" (List.length items);

    let unique_status = List.sort_uniq String.compare (List.map (fun o -> o.status) orders) in
    let unique_origin = List.sort_uniq String.compare (List.map (fun o -> o.origin) orders) in
    Printf.printf "Unique status values: %s\n" (String.concat ", " unique_status);
    Printf.printf "Unique origin values: %s\n" (String.concat ", " unique_origin);

    let status_filter = "Complete" in
    let origin_filter = "O" in
    let filtered_orders = filter_orders orders status_filter origin_filter in
    Printf.printf "Filtered %d orders with status=%s and origin=%s\n" 
      (List.length filtered_orders) status_filter origin_filter;
    
    let joined_data = inner_join filtered_orders items in
    Printf.printf "Joined %d orders with items\n" (List.length joined_data);
    
    let results = process_data orders items status_filter origin_filter in
    Printf.printf "Processed %d results\n" (List.length results);

    write_result "output.csv" results;
    
    (* Adiciona cálculo e escrita das médias mensais *)
    let monthly_avgs = calculate_monthly_averages results orders in
    write_monthly_averages "monthly_averages.csv" monthly_avgs;
    Printf.printf "Generated monthly averages for %d months\n" (List.length monthly_avgs);

    print_endline "ETL concluído!"
  with
  | Sys_error msg -> Printf.printf "Erro ao acessar arquivo: %s\n" msg; exit 1
  | e -> Printf.printf "Erro inesperado: %s\n" (Printexc.to_string e); exit 1