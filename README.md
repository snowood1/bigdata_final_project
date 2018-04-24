# bigdata_final_project


There are three executions phases:

./spark-submit --class "Preprocess" ml_2.11-0.1.jar  train_sample.csv ./out

./spark-submit --jars xgboost4j-spark-0.72-jar-with-dependencies.jar,xgboost4j-0.72-jar-with-dependencies.jar --class "XGBoostML" ml_2.11-0.1.jar  ./out_labelfeatures.parquet/ ./out_pipeline_model/ out_test/ result

./spark-submit --class "Evaluation" ml_2.11-0.1.jar  result_predicts.parquet/


