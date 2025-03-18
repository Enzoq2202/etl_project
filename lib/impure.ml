(** Funções impuras para entrada/saída no projeto ETL. *)

(** Lê ordens de um arquivo CSV e retorna uma lista de records order. *)
let read_orders filename =
  let raw_data = Csv.load filename in
  List.map (fun row ->
    match row with
    | [id; client_id; order_date; status; origin] ->
        { Pure.id = Pure.string_to_int id;
          client_id = Pure.string_to_int client_id;
          order_date; status; origin }
    | _ -> failwith ("Formato inválido para order: " ^ String.concat "," row)
  ) (List.tl raw_data)

(** Lê itens de pedidos de um arquivo CSV e retorna uma lista de records order_item. *)
let read_order_items filename =
  let raw_data = Csv.load filename in
  List.map (fun row ->
    match row with
    | [order_id; product_id; quantity; price; tax] ->
        { Pure.order_id = Pure.string_to_int order_id;
          product_id = Pure.string_to_int product_id;
          quantity = Pure.string_to_int quantity;
          price = Pure.string_to_float price;
          tax = Pure.string_to_float tax }
    | _ -> failwith ("Formato inválido para order_item: " ^ String.concat "," row)
  ) (List.tl raw_data)

(** Escreve os resultados processados em um arquivo CSV com valores formatados. *)
let write_result filename (data : Pure.result list) =
  let header = ["order_id"; "total_amount"; "total_taxes"] in
  let rows = List.map (fun r ->
    [string_of_int r.Pure.order_id; 
     Printf.sprintf "%.2f" r.Pure.total_amount; 
     Printf.sprintf "%.2f" r.Pure.total_taxes]
  ) data in
  Csv.save filename (header :: rows)

(** Escreve as médias mensais em um arquivo CSV. *)
let write_monthly_averages filename (data : Pure.monthly_avg list) =
  let header = ["month_year"; "avg_amount"; "avg_taxes"] in
  let rows = List.map (fun r ->
    [r.Pure.month_year; Printf.sprintf "%.2f" r.Pure.avg_amount; Printf.sprintf "%.2f" r.Pure.avg_taxes]
  ) data in
  Csv.save filename (header :: rows)