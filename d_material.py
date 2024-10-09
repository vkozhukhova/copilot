import argparse
import shlex
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    row_number,
    lit,
    lag,
    when,
    lead,
    current_timestamp,
    to_date,
)
from sources.cargo import ds_address, ds_addresstype
from sources.atlas import (
    ds_z2lis_12_vcitm,
    ds_0material_attr,
    ds_zclf20_text,
    ds_0mat_unit_attr,
    ds_0material_text,
    ds_zbrand_text,
    ds_1cl_audc001,
    ds_1cl_omat001,
    ds_zmktsgt_text,
)
from data_domains.supply_logistics import ds_d_material


def main(spark: SparkSession, path_root: str):
    source_cargo_window = Window().partitionBy("ID").orderBy(col("STAMP").desc(), col("__PIPELINE_UNIX_TS__").desc())
    mat_unit_window = Window.partitionBy("MATNR").orderBy(col("LOAD_DATE").desc())
    mat_attr_window = Window.partitionBy("MATNR_PK").orderBy(col("LOAD_DATE").desc(), col("DI_SEQUENCE_NUMBER").desc())

    df_zclf20_text_src = ds_zclf20_text().read_snapshot(spark)
    df_0mat_unit_attr_src = ds_0mat_unit_attr().read_snapshot(spark)
    df_zbrand_text_src = ds_zbrand_text().read_snapshot(spark)
    df_1cl_audс001_src = ds_1cl_audc001().read_snapshot(spark)
    df_zmktsgt_src = ds_zmktsgt_text().read_snapshot(spark)

    df_address_type_src = (
        ds_addresstype()
        .read_delta(spark)
        .withColumn("rn", row_number().over(source_cargo_window))
        .where((col("rn") == 1) & (col("PREFIX").isin("PLANT", "SHIPPING_POINT")) & col("REMOVEDATE").isNull())
        .drop("rn")
    )

    df_address_trnf = (
        ds_address()
        .read_delta(spark)
        .withColumn("rn", row_number().over(source_cargo_window))
        .where((col("rn") == 1) & col("REMOVEDATE").isNull())
        .drop("rn")
        .alias("df_address_src")
        .join(
            df_address_type_src.alias("df_address_type_src"),
            col("df_address_type_src.ID") == col("df_address_src.ADDRESSTYPEID"),
        )
    )

    df_z2_lis_src = ds_z2lis_12_vcitm().read_history(spark).where((to_date(col("ERDAT"), "yyyy.MM.dd") >= "2019-12-29"))
    df_z2_lis_list = (
        df_z2_lis_src.alias("df_z2_lis_src")
        .join(
            df_address_trnf.alias("df_address_trnf"),
            col("df_address_trnf.SAPID") == col("df_z2_lis_src.WERKS"),
        )
        .where(col("df_z2_lis_src.WADAT_IST") >= "2020-01-01")
        .drop("MATNR")
    )

    df_z2lis_12_vcitm_trnf = (
        df_z2_lis_src.alias("df_z2_lis_src")
        .join(df_z2_lis_list, ["VBELN"])
        .select(
            "df_z2_lis_src.MATNR",
        )
        .distinct()
    )

    df_mat_unit_st_trfn = (
        df_0mat_unit_attr_src.where(col("MEINH") == "ST")
        .withColumn("rn", row_number().over(mat_unit_window))
        .where("rn == 1")
        .drop("rn")
    )

    df_mat_unit_sb_trfn = (
        df_0mat_unit_attr_src.where(col("MEINH") == "SB")
        .withColumn("rn", row_number().over(mat_unit_window))
        .where("rn == 1")
        .drop("rn")
    )

    df_material_text_trfn = ds_0material_text().read_snapshot(spark).where(col("SPRAS") == "EN")

    df_mater_attr_trnf = (
        ds_0material_attr()
        .read_history(spark)
        .withColumn("rn", row_number().over(mat_attr_window))
        .where("rn == 1")
        .drop("rn")
        .select(
            "*",
            (
                when(col("MEABM") == "MM", col("LAENG") / 10)
                .when(col("MEABM") == "M", col("LAENG") * 100)
                .when(col("MEABM") == "IN", col("LAENG") * 2.54)
                .otherwise(col("LAENG"))
            ).alias("LAENG_cm"),
            (
                when(col("MEABM") == "MM", col("BREIT") / 10)
                .when(col("MEABM") == "M", col("BREIT") * 100)
                .when(col("MEABM") == "IN", col("BREIT") * 2.54)
                .otherwise(col("BREIT"))
            ).alias("BREIT_cm"),
            (
                when(col("MEABM") == "MM", col("HOEHE") / 10)
                .when(col("MEABM") == "M", col("HOEHE") * 100)
                .when(col("MEABM") == "IN", col("HOEHE") * 2.54)
                .otherwise(col("HOEHE"))
            ).alias("HOEHE_cm"),
        )
    )

    df_1cl_omat_trnf = (
        ds_1cl_omat001()
        .read_snapshot(spark)
        .alias("df_1cl_omat_src")
        .join(
            df_1cl_audс001_src.alias("df_1cl_audс001_src"),
            col("df_1cl_audс001_src.ATWRT_PK") == col("df_1cl_omat_src.UDCTECH001"),
        )
    )

    df_mater_attr_tgt = (
        df_mater_attr_trnf.alias("df_mater_attr_trnf")
        .join(
            df_z2lis_12_vcitm_trnf.alias("df_z2lis_12_vcitm_trnf"),
            col("df_z2lis_12_vcitm_trnf.MATNR") == col("df_mater_attr_trnf.MATNR_PK"),
        )
        .join(
            df_zclf20_text_src.alias("df_zclf20_text_src"),
            col("df_zclf20_text_src.CHRCOD") == col("df_mater_attr_trnf.ZZCLF20"),
            "left",
        )
        .join(
            df_mat_unit_st_trfn.alias("df_mat_unit_st_trfn"),
            col("df_mat_unit_st_trfn.MATNR") == col("df_mater_attr_trnf.MATNR_PK"),
            "left",
        )
        .join(
            df_mat_unit_sb_trfn.alias("df_mat_unit_sb_trfn"),
            col("df_mat_unit_sb_trfn.MATNR") == col("df_mater_attr_trnf.MATNR_PK"),
            "left",
        )
        .join(
            df_material_text_trfn.alias("df_material_text_trfn"),
            col("df_material_text_trfn.MATNR") == col("df_mater_attr_trnf.MATNR_PK"),
            "left",
        )
        .join(
            df_zbrand_text_src.alias("df_zbrand_text_src"),
            col("df_zbrand_text_src.CHRCOD_PK") == col("df_mater_attr_trnf.ZZCLF03"),
            "left",
        )
        .join(
            df_1cl_omat_trnf.alias("df_1cl_omat_trnf"),
            col("df_1cl_omat_trnf.MATNR_PK") == col("df_mater_attr_trnf.MATNR_PK"),
            "left",
        )
        .join(
            df_zmktsgt_src.alias("df_zmktsgt_src"),
            col("df_zmktsgt_src.CHRCOD_PK") == col("df_mater_attr_trnf.ZZCLF02"),
            "left",
        )
        .select(
            col("df_mater_attr_trnf.MATNR_PK").alias("Material_Code__PK"),
            col("df_mater_attr_trnf.ZZREPMATNR").alias("Material_ZREP"),
            col("df_zclf20_text_src.VTEXT").alias("Material_UnitOfMeasure"),
            col("df_mater_attr_trnf.MEINS").alias("Material_BaseUnitOfMeasure"),
            col("df_mat_unit_st_trfn.UMREN").cast("int").alias("Material_PC2Case"),
            col("df_material_text_trfn.TXTMD").alias("Material_Description"),
            col("df_zbrand_text_src.VTEXT").alias("Brand_Name"),
            col("df_1cl_omat_trnf.TEXT").alias("Technology_Name"),
            col("df_zmktsgt_src.VTEXT").alias("Segment_Name"),
            (
                when(
                    col("df_mater_attr_trnf.GEWEI") == "G",
                    col("df_mater_attr_trnf.NTGEW") / 1000,
                ).otherwise(col("df_mater_attr_trnf.NTGEW"))
            )
            .cast("float")
            .alias("Material_NetWeight__UOM"),
            (
                when(
                    col("df_mater_attr_trnf.GEWEI") == "G",
                    col("df_mater_attr_trnf.BRGEW") / 1000,
                ).otherwise(col("df_mater_attr_trnf.BRGEW"))
            )
            .cast("float")
            .alias("Material_GrossWeight__UOM"),
            (
                col("df_mater_attr_trnf.LAENG_cm")
                * col("df_mater_attr_trnf.BREIT_cm")
                * col("df_mater_attr_trnf.HOEHE_cm")
            ).alias("Material_Volume__UOM"),
            col("df_mater_attr_trnf.MTART").alias("Material_Type"),
            col("df_mater_attr_trnf.SPART").alias("BusinessSegment_Code"),
            (
                when(
                    (
                        lag("df_mater_attr_trnf.LAEDA", 1)
                        .over(
                            Window.partitionBy("df_mater_attr_trnf.MATNR_PK").orderBy(col("df_mater_attr_trnf.LAEDA"))
                        )
                        .isNull()
                    ),
                    lit("2000-01-01"),
                ).otherwise(col("df_mater_attr_trnf.LAEDA"))
            )
            .cast("date")
            .alias("Material_StartDate__PK"),
            (
                when(
                    (
                        lead("df_mater_attr_trnf.LAEDA", 1)
                        .over(
                            Window.partitionBy("df_mater_attr_trnf.MATNR_PK").orderBy(col("df_mater_attr_trnf.LAEDA"))
                        )
                        .isNull()
                    ),
                    lit("9999-12-31"),
                ).otherwise(
                    lead("df_mater_attr_trnf.LAEDA", 1).over(
                        Window.partitionBy("df_mater_attr_trnf.MATNR_PK").orderBy(col("df_mater_attr_trnf.LAEDA"))
                    )
                    - 1
                )
            )
            .cast("date")
            .alias("Material_EndDate"),
            col("df_mat_unit_sb_trfn.UMREN").cast("int").alias("Material_ShowBox__UOM"),
            (
                when(
                    col("df_mat_unit_st_trfn.GEWEI") == "KG",
                    col("df_mat_unit_st_trfn.ZZNTGEW") * 1000,
                ).otherwise(col("df_mat_unit_st_trfn.ZZNTGEW"))
            )
            .cast("double")
            .alias("Material_PieceNetWeight__UOM"),
            col("df_mater_attr_trnf.ZZCLF03").alias("Brand_Code"),
            col("df_1cl_omat_trnf.UDCTECH001").alias("Tech_Code"),
            current_timestamp().alias("_Update_DTTM"),
        )
    )

    (df_mater_attr_tgt.coalesce(2).write.mode("overwrite").parquet(f"{path_root}/{ds_d_material.value}"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--args", nargs=2, action="append")
    all_args = parser.parse_args(shlex.split(" ".join(sys.argv[1:])))

    kwargs = {}
    if all_args.args:
        kwargs = {arg[0]: arg[1] for arg in all_args.args}

    spark = SparkSession.builder.getOrCreate()

    main(spark=spark, **kwargs)
