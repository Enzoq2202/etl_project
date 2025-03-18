open Etl_lib.Pure

let test_calculate_item_total () =
  let item = { order_id = 5; product_id = 202; quantity = 2; price = 161.18; tax = 0.18 } in
  let total = calculate_item_total item in
  assert (total = 322.36);  (* 161.18 * 2 *)
  print_endline "test_calculate_item_total passed"

let test_calculate_item_tax () =
  let item = { order_id = 5; product_id = 202; quantity = 2; price = 161.18; tax = 0.18 } in
  let tax = calculate_item_tax item in
  assert (abs_float (tax -. 58.0248) < 0.0001);  (* 322.36 * 0.18 *)
  print_endline "test_calculate_item_tax passed"

let test_filter_orders () =
  let orders = [
    { id = 2; client_id = 117; order_date = "2024-08-17"; status = "Complete"; origin = "O" };
    { id = 1; client_id = 112; order_date = "2024-10-02"; status = "Pending"; origin = "P" }
  ] in
  let filtered = filter_orders orders "complete" "o" in
  assert (List.length filtered = 1);
  assert ((List.hd filtered).id = 2);
  print_endline "test_filter_orders passed"

let test_group_by_order_id () =
  let items = [
    { order_id = 5; product_id = 202; quantity = 2; price = 161.18; tax = 0.18 };
    { order_id = 5; product_id = 207; quantity = 10; price = 156.33; tax = 0.05 }
  ] in
  let grouped = group_by_order_id items in
  assert (List.length grouped = 1);
  assert (fst (List.hd grouped) = 5);
  assert (List.length (snd (List.hd grouped)) = 2);
  print_endline "test_group_by_order_id passed"

let test_aggregate_totals () =
  let items = [
    { order_id = 5; product_id = 202; quantity = 2; price = 161.18; tax = 0.18 };
    { order_id = 5; product_id = 207; quantity = 10; price = 156.33; tax = 0.05 }
  ] in
  let result = aggregate_totals 5 items in
  assert (abs_float (result.total_amount -. 1885.66) < 0.0001);  (* 322.36 + 1563.30 *)
  assert (abs_float (result.total_taxes -. 136.1898) < 0.0001);  (* 58.0248 + 78.165 *)
  print_endline "test_aggregate_totals passed"

let () =
  test_calculate_item_total ();
  test_calculate_item_tax ();
  test_filter_orders ();
  test_group_by_order_id ();
  test_aggregate_totals ();
  print_endline "All tests passed!"