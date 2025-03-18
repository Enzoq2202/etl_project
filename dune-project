(lang dune 3.17)
(name etl_project)
(generate_opam_files true)

(package
 (name etl_project)
 (synopsis "Um projeto ETL em OCaml para processar pedidos")
 (description "Processa dados de pedidos e itens de CSV, calcula totais e gera um CSV de saída")
 (depends
  ocaml
  dune
  (csv (>= "2.4"))
 )
 (tags ("etl" "functional" "ocaml")))