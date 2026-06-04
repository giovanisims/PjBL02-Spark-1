from pyspark import SparkContext, RDD
# For some reason typechecking is going insane and complaining about everything so I have to cast everything
from typing import Any, Tuple, List, cast

def main():
    sc = SparkContext("local", "PjBL-Spark-Tasks")

    dataset_raw = sc.textFile("dataset.csv")
    # Since we are using RDD/PairRDD for these tasks we need to handle the header manually
    header = dataset_raw.first()
    data: RDD[List[str]] = dataset_raw.filter(lambda line: line != header).map(lambda transaction_row: transaction_row.split(";"))
    
    # Not necessary but speeds up processing since we are reusing data
    data.cache()
    export_results = []
    
    import os
    if not os.path.exists("results"):
        os.makedirs("results")

    def log_result(q_num, title, result):
        header = f"--- {q_num}: {title} ---"
        print(f"\n{header}")
        
        if isinstance(result, list):
            print(f"Resultado (primeiros 10 itens): {result[:10]}")
            content = "\n".join(str(item) for item in result)
        else:
            print(f"Resultado: {result}")
            content = str(result)
            
        with open(f"results/question_{q_num}.txt", "w", encoding="utf-8") as f:
            f.write(f"{header}\n{content}\n")
            
        return f"{header}\nResultado:\n{content}\n\n"

# (0,5 ponto) Número de transações envolvendo o Brasil através de RDD. O retorno deverá ser
# um único valor.

    # The filter operation keeps an internal counter of every datapoint that matches it and that's what count gets
    count_brasil_transactions_RDD = data.filter(lambda transaction: transaction[0] == "Brazil").count()
    export_results.append(log_result(1, "Transações contendo Brasil (RDD)", count_brasil_transactions_RDD))

# (1,0 ponto) Número de transações envolvendo o Brasil utilizando PairRDD. O retorno deverá
# ser composto de chave (Brazil) e valor.

    # The filter is the same but when it finds something matching the filter it creates a key-value pair like "Brazil", 1
    # then for the aggregate by key function the lambda operation adds the value of 1 from each key to a total value
    # the other lambda is to tell spark to sum up the total_value of each data partition 
    count_brasil_transactions_PairRDD = data.filter(lambda transaction: transaction[0] == "Brazil") \
                  .map(lambda _: ("Brazil", int(1))) \
                  .aggregateByKey(0, lambda total_count, current_count: total_count + current_count, lambda partition1, partition2: partition1 + partition2)
    export_results.append(log_result(2, "Transações contendo Brasil (PairRDD)", count_brasil_transactions_PairRDD.collect()))

# (0,5 ponto) Número de transações envolvendo o Brazil durante 2016 utilizando RDD. O
# retorno deverá ser um único valor

    count_brasil_transactions_2016_RDD = data.filter(lambda transaction: transaction[0] == "Brazil" and transaction[1] == "2016").count()
    export_results.append(log_result(3, "Transações contendo Brasil en 2016 (RDD)", count_brasil_transactions_2016_RDD))

# (1,0 ponto) Número de transações envolvendo o Brazil durante 2016 utilizando PairRDD. O
# retorno deverá ser com chave (Brasil, 2016) e valor.

    count_brasil_transactions_2016_PairRDD = data.filter(lambda transaction: transaction[0] == "Brazil" and transaction[1] == "2016") \
                       .map(lambda _: (("Brazil", "2016"), int(1))) \
                       .aggregateByKey(0, lambda total_count, current_count: total_count + current_count, lambda partition1, partition2: partition1 + partition2)
    export_results.append(log_result(4, "Transações contendo Brasil em 2016 (PairRDD)", count_brasil_transactions_2016_PairRDD.collect()))

# (1,0 ponto) Número de transações por flow e por ano ordenados por ano a partir de 2010,
# transformando os valores da coluna Flow em maiúsculas. O retorno será em chave (ano,
# flow) e valor.

    count_transactions_year_flow_PairRDD = data.filter(lambda transaction: transaction[1] != "" and int(transaction[1]) >= 2010) \
                          .map(lambda transaction: ((transaction[1], transaction[4].upper()), int(1))) \
                          .aggregateByKey(0, lambda total_count, current_count: total_count + current_count, lambda partition1, partition2: partition1 + partition2)
    
    count_transactions_year_flow_list = sorted(count_transactions_year_flow_PairRDD.collect())
    export_results.append(log_result(5, "Número de transações por flow e por ano (PairRDD)", count_transactions_year_flow_list))

# (0,5 ponto) A média da coluna Price para o ano de 2016 utilizando RDD. O retorno deverá
# conter apenas o valor da média.

    prices_2016_RDD = data.filter(lambda transaction: transaction[1] == "2016" and transaction[5] != "").map(lambda transaction: float(transaction[5]))
    avg_price_2016_RDD = prices_2016_RDD.sum() / prices_2016_RDD.count()
    export_results.append(log_result(6, "Média da coluna Price para o ano de 2016 (RDD)", avg_price_2016_RDD))

# (1,0 ponto) A média da coluna Price para o ano de 2016 utilizando PairRDD. O retorno deverá
# conter chave (ano) e valor.

    avg_price_2016_PairRDD = data.filter(lambda transaction: transaction[1] == "2016" and transaction[5] != "") \
                        .map(lambda transaction: (transaction[1], (float(transaction[5]), int(1)))) \
                        .aggregateByKey(cast(Tuple[float, int], (0.0, 0)),
                                        lambda total_stats, current_stats: (total_stats[0] + current_stats[0], total_stats[1] + current_stats[1]),
                                        lambda partition1, partition2: (partition1[0] + partition2[0], partition1[1] + partition2[1])) \
                        .mapValues(lambda sum_count: sum_count[0] / sum_count[1])
    export_results.append(log_result(7, "Média da coluna Price para o ano de 2016 (PairRDD)", avg_price_2016_PairRDD.collect()))

# (1,0 ponto) O preço máximo e mínimo por categoria e por ano, ordenado por país, sendo
# que a coluna categoria deve conter valores com letra maiúsculas. Utilizar PairRDD.

    # We have to sort by country but country is not used in the output at all so we hide it
    stats_cat_year_PairRDD = data.filter(lambda transaction: transaction[1] != "" and transaction[9] != "" and transaction[5] != "") \
                         .map(lambda transaction: ((transaction[0], transaction[1], transaction[9].upper()), float(transaction[5]))) \
                         .combineByKey(
                             lambda initial_price: (initial_price, initial_price),
                             lambda current_min_max, next_price: (max(current_min_max[0], next_price), min(current_min_max[1], next_price)),
                             lambda partition1, partition2: (max(partition1[0], partition2[0]), min(partition1[1], partition2[1]))
                         )
    
    # Country is kept as the first element of the key
    stats_list = sorted(stats_cat_year_PairRDD.collect(), key=lambda summary_row: summary_row[0][0])

    final_stats_list = []
    for trade_summary in stats_list:
        display_key = (trade_summary[0][1], trade_summary[0][2])
        display_val = trade_summary[1]
        final_stats_list.append((display_key, display_val))
    
    export_results.append(log_result(8, "Preço máximo e mínimo por categoria e por ano (PairRDD)", final_stats_list))

# (1,0 ponto) O país com o valor máximo de exportação utilizando PairRDD. O retorno deverá
# conter chave (país) e valor.

    max_export_rdd = data.filter(lambda transaction: transaction[4] == "Export" and transaction[5] != "") \
                     .map(lambda transaction: (transaction[0], float(transaction[5]))) \
                     .aggregateByKey(0.0, lambda total_value, current_value: total_value + current_value, lambda partition1, partition2: partition1 + partition2)
    
    max_export = cast(RDD[Tuple[Any, float]], max_export_rdd).max(key=lambda trade: cast(Any, trade[1]))
    export_results.append(log_result(9, "País com valor máximo de exportação (PairRDD)", max_export))

# (1,0 ponto) O preço mínimo por país e por ano, ordenado por ano. Utilizar PairRDD.

    min_price_country_PairRDD = data.filter(lambda transaction: transaction[5] != "") \
                            .map(lambda transaction: ((transaction[1], transaction[0]), float(transaction[5]))) \
                            .aggregateByKey(float('inf'), lambda total_min, current_val: min(total_min, current_val), lambda partition1, partition2: min(partition1, partition2))
    
    min_price_sorted = sorted(min_price_country_PairRDD.collect())
    export_results.append(log_result(10, "Preço mínimo por país e ano (PairRDD)", min_price_sorted))

# (1,0 ponto) A transação com o maior preço por kg na categoria Exportação. O retorno deverá 
# conter o valor do preço/kg e os detalhes (ano, país, categoria).

    max_price_kg_rdd = data.filter(lambda transaction: transaction[4] == "Export" and transaction[6] != "" and float(transaction[6]) > 0 and transaction[5] != "") \
                       .map(lambda transaction: (float(transaction[5]) / float(transaction[6]), (transaction[1], transaction[0], transaction[9])))

    max_price_kg_details = cast(RDD[Tuple[float, Any]], max_price_kg_rdd).max(key=lambda trade: cast(Any, trade[0]))
    export_results.append(log_result(11, "Maior preço/kg exportação (PairRDD)", max_price_kg_details))

    with open("results.txt", "w", encoding="utf-8") as f:
        f.writelines(export_results)

    print("\n--- Resultados (Individuais e Geral) exportados ---")

    sc.stop()


if __name__ == "__main__":
    main()
