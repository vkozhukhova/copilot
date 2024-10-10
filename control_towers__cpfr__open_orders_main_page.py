# Databricks notebook source
# MAGIC %md
# MAGIC ## OPEN ORDERS MAIN PAGE (DECISIONS)
# MAGIC ##### Information about problem open orders and its decision.
# MAGIC ##### Granularity: SalesOrderId - FactPlanBillingDate - Customer - MarsDc - Material
# MAGIC ###
# MAGIC #### Sources: output/RUSSIA_PETCARE_SALES_DM/, output/RUSSIA_PETCARE_SUPPLY_PLANNING_DM/, output/RUSSIA_DATA_FOUNDATION/,
# MAGIC ####          raw/FILES/RUSSIA_PETCARE_SUPPLY_PLANNING_DM/, output/ATLAS/
# MAGIC #### Target:
# MAGIC #### - dev) output/RUSSIA_PETCARE_SUPPLY_PLANNING_DM/CONTROL_TOWERS/OPEN_ORDERS_MAIN_PAGE.PARQUET,
# MAGIC #### - uat) output/RUSSIA_PETCARE_SUPPLY_PLANNING_DM/CONTROL_TOWERS_UAT/OPEN_ORDERS_MAIN_PAGE.PARQUET
# MAGIC ##### Developer: roman.kotyubeev@effem.com

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports and parsing functions

# COMMAND ----------

import argparse
import shlex
import sys
from os import getenv

from pyspark.sql import (
    SparkSession,
    Window,
)
from pyspark.sql.functions import (
    abs as abs_spark,
    array_join,
    avg,
    coalesce,
    col,
    collect_list,
    collect_set,
    concat,
    concat_ws,
    count,
    current_date,
    date_add,
    date_sub,
    dense_rank,
    desc,
    expr,
    length,
    lit,
    max as max_spark,
    min as min_spark,
    regexp_replace,
    round as round_spark,
    row_number,
    size,
    substring,
    sum as sum_spark,
    when,
)
from sources.atlas import ds_z2lis_11_vaitm
from sources.sales_orders import ds_sales_orders_fdm
from sources.universalcatalog import (
    ds_mars_universal_calendar,
    ds_mars_universal_petcare_materials,
)


def main(
    spark: SparkSession,
    s3_prod_env_bucket: str,
    s3_curr_env_bucket: str,
    ct_s3_curr_env_bucket: str,
    pipeline_date: str,
):
    OPEN_ORDERS_PATH = f"s3a://{s3_curr_env_bucket}/application/control_towers/cpfr/datamarts/open_orders/allocation_discontinue_status"
    OO_QUOTAS_PATH = f"s3a://{ct_s3_curr_env_bucket}/application/control_towers/cpfr/datamarts/open_orders/quotas/partition_division=petcare/"
    TARGET_PATH = f"s3a://{ct_s3_curr_env_bucket}/application/control_towers/cpfr/datamarts/open_orders/main_page/partition_division=petcare/"
    FORECAST_PATH = f"s3a://{s3_prod_env_bucket}/application/e2e/kpi/datamarts/d2s_forecast"
    STOCK_GRD_PATH = f"s3a://{s3_prod_env_bucket}/application/e2e/kpi/datamarts/mars_dc_stocks_fg_grd_fdm"
    TRANSIT_PATH = f"s3a://{s3_curr_env_bucket}/application/control_towers/cpfr/source_data/manual_files/transit_days_from_plant_to_wh/transit_days_from_plant_to_wh.csv"
    TRANSPORTATION_DATA = (
        f"s3a://{s3_prod_env_bucket}/application/control_towers/common/datamarts/merged_transportation_data"
    )
    # Константы для фильтрации Z2LIS (VKORG = 261 (Russia), SPART = 5 (Petcare))
    VKORG_FILTERING_RUS_CONST = 261
    SPART_FILTERING_PETCARE_CONST = 5

    # Коэффициент отношения товарного запаса в днях к минимальному стоку,
    # начиная с которого мы считаем, что стока на складе достаточно, чтобы на него можно было перевсести отгрузку материала
    TMS_COEFFICIENT = 0.5

    # COMMAND ----------

    open_orders_source_df = spark.read.parquet(OPEN_ORDERS_PATH)

    # COMMAND ----------

    open_orders_quotas_source_df = spark.read.parquet(OO_QUOTAS_PATH)

    # COMMAND ----------

    # For decision #1
    stock_grd_source_df = spark.read.parquet(STOCK_GRD_PATH)

    # COMMAND ----------

    calendar_source_df = ds_mars_universal_calendar().read_snapshot(spark)

    # COMMAND ----------

    # Выгружаю soh
    soh_df = ds_sales_orders_fdm().read_snapshot(spark)

    # COMMAND ----------

    # For decision #1
    forecast_source_df = spark.read.format("parquet").load(FORECAST_PATH)

    # For decision #2
    transaction_orders_source_df = (
        ds_z2lis_11_vaitm()
        .read_snapshot(spark)
        .where((col("VKORG") == VKORG_FILTERING_RUS_CONST) & (col("SPART") == SPART_FILTERING_PETCARE_CONST))
    )

    # COMMAND ----------

    # For decision #3
    transportation_data_source_df = spark.read.parquet(TRANSPORTATION_DATA)

    # COMMAND ----------

    # For decision #3
    transit_source_df = (
        spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(TRANSIT_PATH)
    )

    # COMMAND ----------

    # For decision #3 and #2
    materials_source_df = ds_mars_universal_petcare_materials().read_snapshot(spark)

    # MAGIC %md
    # MAGIC # Filtering

    # COMMAND ----------

    # Преобразуем из 2022 P6 W4 в 2022P06W4 для форкастов.

    calendar_df = calendar_source_df.withColumn(
        "MARS_WEEK_FOR_DEMAND",
        concat(
            col("MarsYear"),
            when(length("MarsPeriodName") == 2, concat(lit("P0"), substring("MarsPeriodName", 2, 1))).otherwise(
                col("MarsPeriodName")
            ),
            col("MarsWeekName"),
        ),
    )

    curr_period_df = (
        calendar_source_df.where(col("OriginalDate") == pipeline_date)
        .withColumn(
            "CurrentPeriodYear",
            when(col("MarsPeriod") < 10, concat(col("MarsYear"), lit("0"), col("MarsPeriod"))).otherwise(
                concat(col("MarsYear"), col("MarsPeriod"))
            ),
        )
        .withColumn(
            "CurrentPeriodYearWithZeros",
            when(col("MarsPeriod") < 10, concat(col("MarsYear"), lit("00"), col("MarsPeriod"))).otherwise(
                concat(col("MarsYear"), lit("0"), col("MarsPeriod"))
            ),
        )
    )

    first_date_curr_period_df = (
        calendar_source_df.alias("cal")
        .join(
            curr_period_df.alias("curr_period"),
            col("cal.MarsPeriodFullName") == col("curr_period.MarsPeriodFullName"),
            "inner",
        )
        .select(
            min_spark(col("cal.OriginalDate")).alias("FirstDateCurrentPeriod"),
        )
    )

    # COMMAND ----------

    w_last_material = Window.partitionBy("MATERIAL").orderBy(
        desc("START_DATE"),
        desc("VMSTD"),
        desc("0CREATEDON"),
    )

    materials_filtered_df = (
        materials_source_df.withColumn("rn", row_number().over(w_last_material)).where("rn == 1").drop("rn")
    )

    # COMMAND ----------

    # Define wet or dry technology for decison #3

    materials_df = materials_filtered_df.withColumn("UOM_PC2Case", col("UOM_PC2Case").cast("int")).withColumn(
        "Tech",
        when(col("Brand") == "DREAMIES", "DRY")
        .when(col("Technology") == "Dry", "DRY")
        .when(col("Technology") == "Care & Treats", "DRY")
        .when(col("Technology") == "Pouch", "WET")
        .when(col("Technology").isNull(), None)
        .otherwise("Others"),
    )

    # MAGIC %md
    # MAGIC # Decision #2 (Price disagreement)

    # COMMAND ----------

    trans_orders_filter_df = transaction_orders_source_df.select(
        "VBELN",
        "POSNR",
        regexp_replace("MATNR", r"^[0]*", "").alias("MATERIAL_CODE"),
        "VKORG",
        "SPART",
        "BSTNK",
        "KWMENG",
        "UMVKZ",
        "UMVKN",
        "KZWI3",
        "NETPR",
    )

    # COMMAND ----------

    # Фильтрую soh.
    # Отбираю по REJECTIONREASON_TYPE (причина недопоставки) is Null или не ('GOOD', 'DELETED'),
    #     SALESORDERTYPE (тип заказа): ZIC1 и ZOR
    #     GISTATUS == False - берем еще не отгруженные заказы, т.к. по ним мы еще можем скорректировать цену

    soh_filtered_df = soh_df.where(
        (~soh_df["REJECTIONREASON_TYPE"].isin("GOOD", "DELETED") | soh_df["REJECTIONREASON_TYPE"].isNull())
        & soh_df["SALESORDERTYPE"].isin("ZIC1", "ZOR")
        & ~soh_df["GISTATUS"]
    )

    # COMMAND ----------

    # Формируем алерт, если есть несоответсвие в цене: если марсовская цена отличается от ожидаемой клиентской на 2 копейки.

    # Формируем итоговый датафрейм с пересекающимися ключами источников soh и z2lis.
    # (Таким образом дополнительно отфильтровываем документы, несоответствующие условиям из soh_filtered)
    # Подтягиваем необходимые данные из materials_df и производим расчеты ожидаемой клиентской цены,
    # после чего сравниваем ее с марсовской.
    # Если разница между ценами превышает 2 копейки, ставим флаг 1, иначе - 0.
    # В конечном итоге группируем данные до гранулярности MATERIAL_CODE-BSTNK и аггрегируем столбец флага по максимальному значению.

    transaction_orders_df = (
        trans_orders_filter_df.join(
            soh_filtered_df,
            (soh_filtered_df["SALESORDERID"] == trans_orders_filter_df["VBELN"])
            & (soh_filtered_df["POSITION_NUMBER"] == trans_orders_filter_df["POSNR"]),
            "inner",
        )
        .join(materials_df, trans_orders_filter_df["MATERIAL_CODE"] == materials_df["MATERIAL"], "left")
        .select(
            trans_orders_filter_df["MATERIAL_CODE"],
            trans_orders_filter_df["BSTNK"],
            trans_orders_filter_df["VBELN"],
            materials_df["UOM_PC2Case"],
            trans_orders_filter_df["KWMENG"],
            trans_orders_filter_df["UMVKZ"],
            trans_orders_filter_df["UMVKN"],
            trans_orders_filter_df["KZWI3"],
            trans_orders_filter_df["NETPR"],
        )
        .withColumn("CountCases", abs_spark(col("KWMENG") * col("UMVKZ") / col("UMVKN")))
        .withColumn(
            "CustomterExpectedNet",
            abs_spark("KZWI3") / col("CountCases") / coalesce(materials_df["UOM_PC2Case"], lit(1)),
        )
        .withColumn("PriceDisagreement", round_spark(abs_spark(col("CustomterExpectedNet") - abs_spark("NETPR")), 4))
        .withColumn("FlagPriceDisagreement", when(col("PriceDisagreement") > 0.02, 1).otherwise(0))
        .groupBy(
            trans_orders_filter_df["MATERIAL_CODE"],
            trans_orders_filter_df["BSTNK"].alias("PurchaseOrderId"),
        )
        .agg(max_spark("FlagPriceDisagreement").alias("FlagPriceDisagreement"))
    )

    # COMMAND ----------

    # Отбираем строки с флагом 1 и добавляем комментарий

    price_disagreement_alert_df = transaction_orders_df.where(col("FlagPriceDisagreement") == 1).withColumn(
        "AlertPriceDisagreement", lit("Переразместить/скорректировать цену.\n")
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC # Decision #1 (Warehouse loading)

    # COMMAND ----------

    # Описание desision #1.
    # Выполняется только для:
    #    PreSl < 100 % and (PlantName=RU32 or PlantName=RU38) and OrderProcessingDate <=CurrentDate+5
    # Формируем список материалов из заказа SalesOrderID текущей строки для проверки на складах донорах
    # Обращаемся к таблице складов доноров (plants_df)
    # Проверяем условие для всего списка материалов на уровне склада донора:
    #   ((free stock (absolute) - ordered текущий заказ (в CS)) / forecast 8 week per day (всего склада)) / TMS >= 100%
    # Если весь список материалов удовлетворил условию, то формируем алёрт
    # В случае, если для RU32 оба склада донора удовлетворяют условию, то
    #   попозиционно сравниваем free stock (absolute) одного склада с другим;
    #   выбираем тот склад, где больше единиц;
    #   в  случае равенства единиц , в обоих складах, выбираем тот склад сумма free stock (absolute) которого по всем позициям из списка больше;
    #   если free sotck одинаковый, то берем RU37
    # Формируем алёрт "Перенести отгрузку заказа на склад RU..."

    open_orders_price_disagreement_alert_df = open_orders_source_df.join(
        price_disagreement_alert_df,
        (open_orders_source_df["Material"] == price_disagreement_alert_df["MATERIAL_CODE"])
        & (open_orders_source_df["PurchaseOrderId"] == price_disagreement_alert_df["PurchaseOrderId"]),
        "left",
    ).select(
        open_orders_source_df["*"],
        price_disagreement_alert_df["FlagPriceDisagreement"],
        price_disagreement_alert_df["AlertPriceDisagreement"],
    )

    w_saleorder = Window.partitionBy("SalesOrderId")

    w_saleorder_grd = Window.partitionBy("SalesOrderId", "Material")

    date_ahead = date_add(current_date(), 5)

    slicepallet_threshold = 0.3

    target_orders_df = (
        open_orders_price_disagreement_alert_df.withColumn(
            "CutQtyPalletClean",
            when(
                ((col("Allocation").isNull()) | (col("AllocationAvailable") == 0))
                & (col("FlagPriceDisagreement").isNull()),
                col("CUT_QTY_PALLET"),
            ),
        )
        .withColumn("CsPreSlSalesOrder", avg(when(col("CsPreSl") < 1, lit(0)).otherwise(lit(1))).over(w_saleorder))
        .withColumn(
            "SlicePalletInSalesOrder",
            coalesce(
                sum_spark("CutQtyPalletClean").over(w_saleorder)
                / sum_spark("AGREED_ORDEREDQTY_PALLET").over(w_saleorder),
                lit(0),
            ),
        )
        .where(
            (col("CsPreSlSalesOrder") < 1)
            & (col("MarsDc").isin("RU32", "RU38", "RU37"))
            & (col("OrderProcessingDate") <= date_ahead)
            & (col("SlicePalletInSalesOrder") >= slicepallet_threshold)
        )
        .select(
            "SalesOrderId",
            "MarsDc",
            "FactPlanBillingDate",
            "CsOrdered",
            "CsConfirmed",
            "Material",
            "Customer",
            "MarsWeekFullName",
        )
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Donor plants

    # COMMAND ----------

    plants_df = spark.createDataFrame(
        [
            ("RU32", "RU37"),
            ("RU32", "RU38"),
            ("RU38", "RU32"),
            ("RU37", "RU32"),
        ],
        ("SourcePlant", "DonorPlant"),
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Latest Stock

    # COMMAND ----------

    # Берем сток за самую последнюю дату.

    w_plant_material_by_last_day = Window.partitionBy("WAREHOUSE", "MATERIAL_CODE").orderBy(desc("DAY"))

    last_stock_grd_df = stock_grd_source_df.withColumn("rn", row_number().over(w_plant_material_by_last_day)).where(
        "rn == 1"
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Projection of stock by day for the future

    # COMMAND ----------

    # Логика джойна с форкастами взята из ноутбука E2E_DC_FG_STOCKS_GRD_ZREP_FDM. Там только посчитано для ZREP.
    # Сначала джойнится календарь со стоком так, что берутся следующие 56 дней. Таким образом, каждую запись
    # стока мы увеличиваем в 56 раз, единственные отличные поле будут OriginalDate и MARS_WEEK.
    # Затем джойним с форкастами, агрегируем по дням и делим форкасты на 7.

    stock_grd_agg_df = (
        last_stock_grd_df.join(
            calendar_df,
            calendar_df["OriginalDate"].between(last_stock_grd_df["DAY"], date_add(last_stock_grd_df["DAY"], 56)),
        )
        .join(
            forecast_source_df,
            (last_stock_grd_df["WAREHOUSE"] == forecast_source_df["PLANT"])
            & (last_stock_grd_df["MATERIAL_CODE"] == forecast_source_df["MATERIAL_CODE"])
            & (calendar_df["MARS_WEEK_FOR_DEMAND"] == forecast_source_df["MARS_WEEK"])
            & (
                last_stock_grd_df["DAY"].between(
                    forecast_source_df["F_STARTDATE"], date_add(forecast_source_df["F_ENDDATE"], -1)
                )
            ),
            "left",
        )
        .groupBy(
            last_stock_grd_df["TMS"],
            last_stock_grd_df["FREE_STOCK"],
            last_stock_grd_df["WAREHOUSE"],
            last_stock_grd_df["MATERIAL_CODE"],
            last_stock_grd_df["DAY"],
            calendar_df["OriginalDate"],
        )
        .agg(
            coalesce(sum_spark(forecast_source_df["FORECAST_CASES"] / 7), lit(0)).alias("FORECAST_CASES"),
        )
    )

    # COMMAND ----------

    # Берем сумму прогнозов за 56 дней, затем делим это на 56.
    # Получаем средний прогноз на день за 56 дней.

    wh_grd_day = ["WAREHOUSE", "MATERIAL_CODE", "DAY"]

    w_wh_grd_day = Window.partitionBy(wh_grd_day)

    w_wh_grd_day_by_date = Window.partitionBy(wh_grd_day).orderBy(desc("OriginalDate"))

    days_in_8_weeks = 56

    stock_grd_df = (
        stock_grd_agg_df.withColumn("accumulativeSum", sum_spark("FORECAST_CASES").over(w_wh_grd_day))
        .withColumn("rn", row_number().over(w_wh_grd_day_by_date))
        .where("rn = 1")
        .withColumn("FORECAST_CASES_8_WEEK_BY_PER_DAY", col("accumulativeSum") / days_in_8_weeks)
        .drop("rn")
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Joining stock with target orders

    # COMMAND ----------

    # NOTE: сток мы джойним к складу-донору.

    target_orders_plant_df = (
        target_orders_df.join(plants_df, target_orders_df["MarsDc"] == plants_df["SourcePlant"], "left")
        .join(
            stock_grd_df,
            (stock_grd_df["WAREHOUSE"] == plants_df["DonorPlant"])
            & (target_orders_df["Material"] == stock_grd_df["MATERIAL_CODE"]),
            "left",
        )
        .withColumn(
            "PerformanceFreeStock",
            (stock_grd_df["FREE_STOCK"] - target_orders_df["CsOrdered"])
            / stock_grd_df["FORECAST_CASES_8_WEEK_BY_PER_DAY"],
        )
        .withColumn(
            "AlertForGRD", when(col("PerformanceFreeStock") / stock_grd_df["TMS"] >= TMS_COEFFICIENT, 1).otherwise(0)
        )
        .select(
            target_orders_df["*"],
            plants_df["DonorPlant"],
            stock_grd_df["TMS"],
            stock_grd_df["FREE_STOCK"],
            stock_grd_df["FORECAST_CASES_8_WEEK_BY_PER_DAY"],
            col("PerformanceFreeStock"),
            col("AlertForGRD"),
        )
    )

    # COMMAND ----------

    # 1. Выбираем только те заказы, у которых каждый материал может быть отгружен хотя бы из одного склада донора (col('SumAlertsForGRD') == col('CountMaterialsInSalesorder')),
    #   при этом гарантируется целостность заказа, если один или несколько материалов в заказе нельзя отгрузить ни с одного склада донора, такой заказ отбрасывается полностью
    # 2. В оставшемся массиве оставляем только записи с AlertForGRD = 1
    # 3. Считаем для каждого склада донора в заказе количество материалов, которые с него можно отгрузить и
    #   сравниваем их с общим количеством уникальных материалов в заказе (col('CountMaterialsInDonorPlant') == col('CountUniqueMaterialsInSalesorder'))
    # 4. Если это количество не сошлось - отбрасываем этот заказ, т.к. его нельзя в полном объеме отгрузить ни с одного склада
    # 5. В итоге получаем массив с заказами, которые полностью можно отгрузить с одного или нескольких складов-доноров

    w_saleorder_donor_plant = Window.partitionBy("SalesOrderId", "DonorPlant")

    salesorder_alerts_df = (
        target_orders_plant_df.withColumn("AnyGRDPlantAlert", max_spark("AlertForGRD").over(w_saleorder_grd))
        .withColumn("CountMaterialsInSalesorder", count("Material").over(w_saleorder))
        .withColumn("SumAlertsForGRD", sum_spark("AnyGRDPlantAlert").over(w_saleorder))
        .where((col("SumAlertsForGRD") == col("CountMaterialsInSalesorder")) & (col("AlertForGRD") == 1))
        .withColumn("CountMaterialsInDonorPlant", count("Material").over(w_saleorder_donor_plant))
        .withColumn("CountUniqueMaterialsInSalesorder", size(collect_set("Material").over(w_saleorder)))
        .where((col("CountMaterialsInDonorPlant") == col("CountUniqueMaterialsInSalesorder")))
        .withColumn(
            "CsPreSlSalesOrder", sum_spark("CsConfirmed").over(w_saleorder) / sum_spark("CsOrdered").over(w_saleorder)
        )
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC ## Decisions for Plants

    # COMMAND ----------

    # 1. Ранжируем склады доноры в разрезе Заказ-Материал и отбираем те, у которых FREE_STOCK больше
    # 2. Схлопываем материалы и подсчитываем количество случаев для каждого склада донора в заказе, когда FREE_STOCK у него оказался больше, чем у другого
    # 3. Отбираем тот склад, у которого по итогу оказалось наибольшее количество таких случаев, при этом если у нас склады по этому признаку равны, то выводим в алерт их все

    w_saleorder_grd_sort_free_stock = w_saleorder_grd.orderBy(desc("FREE_STOCK"))

    w_saleorder_sort_count_cases = w_saleorder.orderBy(desc("CountCasesDonorPlantGreatest"))

    plant_loading_alert_df = (
        salesorder_alerts_df.withColumn("rn", row_number().over(w_saleorder_grd_sort_free_stock))
        .filter(col("rn") == 1)
        .groupBy(
            "SalesOrderId",
            "CsPreSlSalesOrder",
            "MarsDc",
            "DonorPlant",
        )
        .agg(sum_spark("rn").alias("CountCasesDonorPlantGreatest"))
        .withColumn("RnCountCasesDonorPlantGreatest", dense_rank().over(w_saleorder_sort_count_cases))
        .filter(col("RnCountCasesDonorPlantGreatest") == 1)
        .groupBy(
            "SalesOrderId",
            "CsPreSlSalesOrder",
        )
        .agg(collect_list("DonorPlant").alias("DonorPlant"))
        .withColumn(
            "DonorPlantStr",
            array_join(
                col("DonorPlant"),
                " или ",
            ),
        )
        .withColumn("SL_growth", round_spark(expr("(1 - CsPreSlSalesOrder) * 100"), 0).cast("int"))
        .withColumn(
            "Alert",
            concat(
                lit("Перенести отгрузку заказа на склад "),
                col("DonorPlantStr"),
                lit(". Уровень сервиса повысится на "),
                col("SL_growth"),
                lit("%\n"),
            ),
        )
        .withColumn("AlertPriority", lit(6))
        .select(
            "SalesOrderId",
            "Alert",
            "AlertPriority",
        )
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC # Decision #3 (Moving date loading on date restock)

    # COMMAND ----------

    # Данные транспортировок из фабрик/копакинг складов в скалды Марса.
    # Сначала агрегируем все накладные, затем джойним количество дней транзита и контроля качества.
    # И получаем транзитный день - это дата, когда прибудет товар на склад.
    # Отфильтровываем данные, начиная с первого дня текущего периода, так как ОО также с него начинаются.
    transportation_data_df = (
        transportation_data_source_df.where(col("Division") == "Petcare")
        .groupBy(
            "Delivery_ActualGI__DTTM",
            "GRD",
            "Delivery_LocationFrom__FK",
            "Delivery_LocationTo__FK",
        )
        .agg(
            sum_spark("GoodsIssue_Qty__CS").alias("GoodsIssue_Qty__CS"),
        )
        .join(
            first_date_curr_period_df,
            transportation_data_source_df["Delivery_ActualGI__DTTM"]
            >= first_date_curr_period_df["FirstDateCurrentPeriod"],
            "inner",
        )
        .join(
            materials_df,
            transportation_data_source_df["GRD"] == materials_df["MATNR"],
            "inner",
        )
        .join(
            transit_source_df,
            (transportation_data_source_df["Delivery_LocationFrom__FK"] == transit_source_df["from2"])
            & (transportation_data_source_df["Delivery_LocationTo__FK"] == transit_source_df["to4"])
            & (materials_df["Tech"] == transit_source_df["Tech"]),
            "inner",
        )
        .withColumn(
            "TransitDate",
            transportation_data_source_df["Delivery_ActualGI__DTTM"] + transit_source_df["TransitQIdays"].cast("int"),
        )
        .select(
            transportation_data_source_df["Delivery_ActualGI__DTTM"],
            transportation_data_source_df["GRD"],
            transportation_data_source_df["Delivery_LocationFrom__FK"],
            transportation_data_source_df["Delivery_LocationTo__FK"],
            col("TransitDate"),
        )
    )

    # COMMAND ----------

    # Получаем заказы, которые подпдают под условие Decision
    # Затем джойним данные полученных транзитов.
    # Будут дубли, так как на один товар-склад множество дат, поэтому выбираем самую первую,
    # которая больше FactPlanBillingDate.
    days_ahead_for_processing_date = date_add(current_date(), 3)
    presl_threshold = 1
    slicepallet_threshold = 0.1

    window_sales_order_by_transit = Window.partitionBy("SalesOrderId", "PositionNumber").orderBy(
        transportation_data_df["TransitDate"]
    )

    stock_alert_df = (
        open_orders_source_df.where(
            (col("CsPreSl") < presl_threshold)
            & (col("Discontinue") == "No")
            & (col("SlicePallet") >= slicepallet_threshold)
            & (col("OrderProcessingDate") <= days_ahead_for_processing_date)
        )
        .join(
            transportation_data_df,
            (open_orders_source_df["Material"] == transportation_data_df["GRD"])
            & (open_orders_source_df["MarsDc"] == transportation_data_df["Delivery_LocationTo__FK"])
            & (open_orders_source_df["FactPlanBillingDate"] < transportation_data_df["TransitDate"]),
            "left",
        )
        .withColumn(
            "rn",
            row_number().over(window_sales_order_by_transit),
        )
        .where(col("rn") == 1)
        .withColumn(
            "Alert",
            when(
                transportation_data_df["TransitDate"].isNotNull(),
                concat(
                    lit("Перенести дату отгрузки на дату пополнения "), transportation_data_df["TransitDate"], lit("\n")
                ),
            ).otherwise(lit("Товара нет в плане производства на текущей неделе.\n")),
        )
        .select(
            open_orders_source_df["SalesOrderId"],
            open_orders_source_df["Material"],
            open_orders_source_df["FactPlanBillingDate"],
            open_orders_source_df["PositionNumber"],
            open_orders_source_df["MarsDc"],
            col("Alert"),
            lit(5).alias("AlertPriority"),
        )
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC # Decision #4 (Quota exceeding)

    # COMMAND ----------

    # Берем квоту последней версии.

    quota_exceeding_alert_df = (
        open_orders_quotas_source_df.where(
            (col("ForecastVersionStartDate") <= pipeline_date)
            & (col("ForecastVersionEndDate") > pipeline_date)
            & (col("RemainingUnconfirmedQuota") > 0)
        )
        .withColumn(
            "Alert",
            concat(
                lit("Имеется превышение квоты на "),
                col("RemainingUnconfirmedQuota"),
                lit(" cases.\n"),
                lit("ТОП-4 клиента с наибольшей свободной квотой:\n"),
                col("Summary"),
                lit("\n"),
            ),
        )
        .select(
            col("RemainingUnconfirmedQuota"),
            col("Material").alias("Alert4_Material"),
            col("Customer").alias("Alert4_Customer"),
            col("Summary"),
            col("Alert"),
            lit(4).alias("AlertPriority"),
        )
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC # Decision #5 (Quota in far order)

    # COMMAND ----------

    # Определяем заказы, для которых формируем алерты.
    # Затем находим такие же материалы с клиентами в других заказах,
    # т.е. дальних (target_salesorders_df).

    w_last_material_in_salesorder = Window.partitionBy("SalesOrderId", "Material", "SoldTo").orderBy(
        desc("FactPlanBillingDate")
    )

    alert_salesorders_df = (
        open_orders_source_df.withColumn(
            "rn",
            row_number().over(w_last_material_in_salesorder),
        )
        .where(
            (col("rn") == 1)
            & (col("Allocation") == True)
            & (col("CsConfirmed") == 0)
            & (col("OrderProcessingDate") >= date_sub(current_date(), 2))
        )
        .select(
            col("FactPlanBillingDate").alias("A_FactPlanBillingDate"),
            col("OrderCreationDate").alias("A_OrderCreationDate"),
            col("SalesOrderId").alias("A_SalesOrderId"),
            col("SoldTo").alias("A_SoldTo"),
            col("Material").alias("A_Material"),
            col("CsConfirmed").alias("A_CsConfirmed"),
        )
    )

    target_salesorders_df = (
        open_orders_source_df.withColumn(
            "rn",
            row_number().over(w_last_material_in_salesorder),
        )
        .where((col("rn") == 1) & (col("FactPlanBillingDate") > date_add(current_date(), 4)) & (col("CsConfirmed") > 0))
        .select(
            col("FactPlanBillingDate").alias("T_FactPlanBillingDate"),
            col("OrderCreationDate").alias("T_OrderCreationDate"),
            col("SalesOrderId").alias("T_SalesOrderId"),
            col("SoldTo").alias("T_SoldTo"),
            col("Material").alias("T_Material"),
            col("CsConfirmed").alias("T_CsConfirmed").cast("int"),
        )
    )

    # COMMAND ----------

    quota_in_far_salesorder_df = (
        alert_salesorders_df.join(
            target_salesorders_df,
            (alert_salesorders_df["A_SoldTo"] == target_salesorders_df["T_SoldTo"])
            & (alert_salesorders_df["A_Material"] == target_salesorders_df["T_Material"])
            & (alert_salesorders_df["A_SalesOrderId"] != target_salesorders_df["T_SalesOrderId"])
            & (alert_salesorders_df["A_OrderCreationDate"] >= target_salesorders_df["T_OrderCreationDate"])
            & (
                date_add(alert_salesorders_df["A_FactPlanBillingDate"], 1)
                < target_salesorders_df["T_FactPlanBillingDate"]
            ),
            "inner",
        )
        .select(
            alert_salesorders_df["A_SalesOrderId"],
            alert_salesorders_df["A_Material"],
            alert_salesorders_df["A_SoldTo"],
            alert_salesorders_df["A_FactPlanBillingDate"],
            alert_salesorders_df["A_OrderCreationDate"],
            target_salesorders_df["T_SalesOrderId"],
            target_salesorders_df["T_FactPlanBillingDate"],
            target_salesorders_df["T_OrderCreationDate"],
            target_salesorders_df["T_CsConfirmed"],
        )
        .groupBy(
            "A_SalesOrderId",
            "A_Material",
            "A_SoldTo",
        )
        .agg(
            concat_ws(
                "\n ",
                lit("Квота в дальнем заказе, необходимо снять с заказов:"),
                collect_list(concat(col("T_SalesOrderId"), lit(" ("), col("T_CsConfirmed"), lit(" cases)"))),
                lit("\n"),
            ).alias("Alert")
        )
        .withColumn(
            "AlertPriority",
            lit(3),
        )
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC # Joining

    # COMMAND ----------

    open_orders_with_alerts_df = open_orders_price_disagreement_alert_df.select(
        col("*"),
        when(col("Discontinue") == "Yes", lit(2)).alias("DiscontinueAlertPriority"),
        when(col("Discontinue") == "Yes", lit("Внимание. Заказан продукт выведенный из ассортимента"))
        .otherwise(lit(""))
        .alias("DiscontinueAlert"),
    )

    # COMMAND ----------

    # Приоритеты алертов (Decisions):
    # 0 - нет алерта
    # 1 - Переразместить/скорректировать цену
    # 2 - Внимание. Заказан неактивный код
    # 3 - Квота в дальнем заказе, необходимо снять с заказа
    # 4 - Превышение квоты
    # 5 - Перенести дату отгрузки на дату пополнения/Товара нет в плане производства
    # 6 - Перенести отгрузку заказа на склад
    # 7 - Отсутствует прогноз - необходимо проверить прогноз
    # 8 - BBD, или микрохолд (нужно проверить статус продукта на складе)

    open_orders_df = (
        open_orders_with_alerts_df.alias("orders")
        .join(
            plant_loading_alert_df,
            open_orders_with_alerts_df["SalesOrderId"] == plant_loading_alert_df["SalesOrderId"],
            "left",
        )
        .join(
            stock_alert_df.alias("stock_alert"),
            (col("orders.Material") == col("stock_alert.Material"))
            & (col("orders.PositionNumber") == col("stock_alert.PositionNumber"))
            & (col("orders.MarsDc") == col("stock_alert.MarsDc"))
            & (col("orders.SalesOrderId") == col("stock_alert.SalesOrderId")),
            "left",
        )
        .join(
            quota_in_far_salesorder_df,
            (open_orders_with_alerts_df["SalesOrderId"] == quota_in_far_salesorder_df["A_SalesOrderId"])
            & (open_orders_with_alerts_df["Material"] == quota_in_far_salesorder_df["A_Material"])
            & (open_orders_with_alerts_df["SoldTo"] == quota_in_far_salesorder_df["A_SoldTo"]),
            "left",
        )
        .join(
            quota_exceeding_alert_df,
            (open_orders_with_alerts_df["Allocation"] == True)
            & (open_orders_with_alerts_df["AllocationAvailable"] == 0)
            & (open_orders_with_alerts_df["Material"] == quota_exceeding_alert_df["Alert4_Material"])
            & (open_orders_with_alerts_df["Customer"] == quota_exceeding_alert_df["Alert4_Customer"]),
            "left",
        )
        .join(
            materials_df,
            open_orders_with_alerts_df["Material"] == materials_df["MATERIAL"],
            "left",
        )
        .withColumn(
            "Decision",
            coalesce(
                open_orders_with_alerts_df["FlagPriceDisagreement"],
                open_orders_with_alerts_df["DiscontinueAlertPriority"],
                quota_in_far_salesorder_df["AlertPriority"],
                quota_exceeding_alert_df["AlertPriority"],
                stock_alert_df["AlertPriority"],
                # Number 7 is still in progress
                # Number 8 is still in progress
                lit(0),
            ),
        )
        .withColumn(
            "DecisionDescription",
            concat(
                coalesce(open_orders_with_alerts_df["AlertPriceDisagreement"], lit("")),
                open_orders_with_alerts_df["DiscontinueAlert"],
                coalesce(quota_in_far_salesorder_df["Alert"], lit("")),
                coalesce(quota_exceeding_alert_df["Alert"], lit("")),
                coalesce(stock_alert_df["Alert"], lit("")),
                # Number 7 is still in progress
                # Number 8 is still in progress
            ),
        )
        .withColumn("FlagPriceDisagreement", coalesce(open_orders_with_alerts_df["FlagPriceDisagreement"], lit(0)))
        .select(
            open_orders_source_df["*"],
            col("FlagPriceDisagreement"),
            col("Decision"),
            col("DecisionDescription"),
            col("Brand"),
        )
    )

    # COMMAND ----------

    # MAGIC %md
    # MAGIC # Saving result + fake promo

    # COMMAND ----------

    # NOTE: поля Promo пока нет, поэтому ставим заглушки.

    final_typed_df = open_orders_df.withColumn("Promo", lit(0)).select(
        col("Customer").cast("varchar(50)"),
        col("CustomerProductId").cast("varchar(50)"),
        col("Division").cast("varchar(50)"),
        col("MarsDc").cast("varchar(50)"),
        col("CustomerDc").cast("varchar(50)"),
        col("CustomerDcCode").cast("varchar(50)"),
        col("Promo").cast("boolean"),
        col("MarsWeekFullName").cast("varchar(50)"),
        col("MarsWeekRollingNum").cast("int"),
        col("MarsDay").cast("int"),
        col("Region").cast("varchar(50)"),
        col("Brand").cast("varchar(50)"),
        concat_ws(" ", col("Brand"), col("Technology")).alias("Subbrand"),
        col("Technology").cast("varchar(50)"),
        col("LsvOrdered").cast("double"),
        col("LsvConfirmed").cast("double"),
        col("LsvPreSl").cast("double"),
        col("LsvPreUnderdeliver").cast("double"),
        col("CsOrdered").cast("double"),
        col("CsConfirmed").cast("double"),
        col("CsPreSl").cast("double"),
        col("CsPreUnderdeliver").cast("double"),
        col("UnitsOrdered").cast("double"),
        col("UnitsConfirmed").cast("double"),
        col("UnitsPreSl").cast("double"),
        col("UnitsPreUnderdeliver").cast("double"),
        col("FactPlanBillingDate").cast("date"),
        col("OrderProcessingDate").cast("date"),
        col("OrderCreationDate").cast("date"),
        col("RequestedDeliveryDate").cast("date"),
        col("Material").cast("varchar(50)"),
        col("Zrep").cast("varchar(20)"),
        col("MaterialDescription").cast("varchar(100)"),
        lit(None).alias("SiKg").cast("double"),
        lit(None).alias("SiRur").cast("double"),
        lit(None).alias("SiPcs").cast("double"),
        lit(None).alias("SiCase").cast("double"),
        col("Status").cast("varchar(50)"),
        col("PositionNumber").cast("varchar(10)"),
        col("SalesOrderId").cast("varchar(50)"),
        col("PurchaseOrderId").cast("varchar(50)"),
        col("Delivery").cast("boolean"),
        col("Slices").cast("boolean"),
        col("TMSWeighted").cast("double"),
        col("PerformanceClient").cast("double"),
        col("PerformanceCountry").cast("double"),
        col("PerformanceClientPlant").cast("double"),
        col("CaseFillClient").cast("double"),
        col("CaseFillCountry").cast("double"),
        col("SFAClient").cast("double"),
        col("SlicePallet").cast("double"),
        col("SliceCase").cast("double"),
        col("SliceFlag").cast("boolean"),
        col("Allocation").cast("boolean"),
        col("AllocationAvailable").cast("double"),
        col("Discontinue").cast("varchar(50)"),
        col("CurrentStatus").cast("varchar(400)"),
        col("FlagPriceDisagreement").cast("boolean"),
        col("DecisionDescription").cast("varchar(100)"),
        col("Decision").cast("int"),
    )

    # COMMAND ----------

    final_typed_df.write.mode("overwrite").parquet(TARGET_PATH)


# -END main-----------------------------------------


if __name__ == "__main__":
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser()
    parser.add_argument("-job", required=True)
    parser.add_argument("-name", required=True)
    parser.add_argument("--args", nargs=2, action="append")
    all_args = parser.parse_args(shlex.split(" ".join(sys.argv[1:])))

    kwargs = {}
    if all_args.args:
        kwargs = {arg[0]: arg[1] for arg in all_args.args}

    # создание spark context
    spark = SparkSession.builder.appName(all_args.name).getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # вызов функции main() с передачей аргументов командной строки
    main(
        spark=spark,
        s3_prod_env_bucket=getenv("S3_PROD_ENV_BUCKET"),
        s3_curr_env_bucket=getenv("S3_CURR_ENV_BUCKET"),
        ct_s3_curr_env_bucket=getenv("CT_S3_CURR_ENV_BUCKET"),
        **kwargs,
    )
