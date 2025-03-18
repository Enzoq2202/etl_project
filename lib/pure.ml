(** Funções puras para transformação de dados no projeto ETL. *)

(** Tipo representando um pedido. *)
type order = { id : int; client_id : int; order_date : string; status : string; origin : string }

(** Tipo representando um item de pedido. *)
type order_item = { order_id : int; product_id : int; quantity : int; price : float; tax : float }

(** Tipo representando o resultado processado. *)
type result = { order_id : int; total_amount : float; total_taxes : float }

(** Converte uma string para inteiro. *)
let string_to_int s = int_of_string s

(** Converte uma string para float. *)
let string_to_float s = float_of_string s

(** Calcula o total de um item multiplicando preço por quantidade. *)
let calculate_item_total (item : order_item) : float = item.price *. float_of_int item.quantity

(** Calcula o imposto de um item como total multiplicado pelo percentual de imposto. *)
let calculate_item_tax (item : order_item) : float = calculate_item_total item *. item.tax

(** Filtra ordens por status e origem, ignorando maiúsculas/minúsculas. *)
let filter_orders (orders : order list) (status : string) (origin : string) : order list =
  let status = String.lowercase_ascii status in
  let origin = String.lowercase_ascii origin in
  List.filter (fun o -> 
    String.lowercase_ascii o.status = status && String.lowercase_ascii o.origin = origin
  ) orders

(** Agrupa itens por order_id usando fold_left. *)
let group_by_order_id (items : order_item list) : (int * order_item list) list =
  List.fold_left (fun (acc : (int * order_item list) list) (item : order_item) ->
    let key = item.order_id in
    let group = try List.assoc key acc with Not_found -> [] in
    (key, item :: group) :: List.remove_assoc key acc
  ) [] items |> List.map (fun (k, v : int * order_item list) -> (k, List.rev v))

(** Realiza um inner join entre ordens e itens agrupados por order_id. *)
let inner_join (orders : order list) (order_items : order_item list) : (order * order_item list) list =
  let grouped_items : (int * order_item list) list = group_by_order_id order_items in
  List.filter_map (fun (order : order) ->
    try Some (order, List.assoc order.id grouped_items) with Not_found -> None
  ) orders

(** Agrega totais de um pedido, somando receita e impostos dos itens. *)
let aggregate_totals (order_id : int) (items : order_item list) : result =
  let total_amount = List.fold_left (fun acc item -> acc +. calculate_item_total item) 0.0 items in
  let total_taxes = List.fold_left (fun acc item -> acc +. calculate_item_tax item) 0.0 items in
  { order_id; total_amount; total_taxes }

(** Processa os dados: filtra ordens, faz join com itens e agrega totais. *)
let process_data (orders : order list) (order_items : order_item list) (status : string) (origin : string) : result list =
  let filtered_orders : order list = filter_orders orders status origin in
  let joined_data : (order * order_item list) list = inner_join filtered_orders order_items in
  List.map (fun (order, items : order * order_item list) -> aggregate_totals order.id items) joined_data

(** Tipo representando médias mensais de receita e impostos. *)
type monthly_avg = { month_year : string; avg_amount : float; avg_taxes : float }

(** Extrai mês e ano de uma data no formato "YYYY-MM-DDThh:mm:ss". *)
let extract_month_year (order_date : string) : string =
  String.sub order_date 0 7  (* "YYYY-MM" *)

(** Calcula médias de receita e impostos agrupadas por mês e ano. *)
let calculate_monthly_averages (results : result list) (orders : order list) : monthly_avg list =
  let with_dates = List.map (fun r ->
    let order = List.find (fun o -> o.id = r.order_id) orders in
    (extract_month_year order.order_date, r.total_amount, r.total_taxes)
  ) results in
  let grouped = List.fold_left (fun acc (my, amt, tax) ->
    let (count, sum_amt, sum_tax) = try List.assoc my acc with Not_found -> (0, 0.0, 0.0) in
    (my, (count + 1, sum_amt +. amt, sum_tax +. tax)) :: List.remove_assoc my acc
  ) [] with_dates in
  List.map (fun (my, (count, sum_amt, sum_tax)) ->
    { month_year = my; avg_amount = sum_amt /. float_of_int count; avg_taxes = sum_tax /. float_of_int count }
  ) grouped