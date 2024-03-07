# Databricks notebook source

import pyspark.sql.functions as F
from datetime import datetime, timedelta

# COMMAND ----------

#constants 
tags_prefix = 'AMI-CONV.L'

#define a dictionary for converter constants
converter_constants = {
    "belt_1" : 2,
    "belt_2" : 1.7,
    "belt_3": 2.05,
    "z_pitch": 0.047625,
    "z_num_lanes": 12,
    "belt_draw_percent": 10,
    "d_pouch_drop_time_in_s": 0.65,
    "reject_drop_off": 4
}

converter_op_row_index = {
    "bottom_dosing": 458,
    "top_dosing": 470,
    "top_combining": 472,
    "bottom_solvent": 481,
    "nd_combining": 488,
    "md_cutter": 504,
    "cd_cutter": 538,
    "reject_valves": 547,
    "bottom_thermoforming": 444,
    "top_thermoforming": 438,
    "top_solvent": 468,
    "bottom_splice_box": 71,
    "top_splice_box": 352,
    "middle_splice_box":370,
    "bottom_splice_detect":105,
    "middle_splice_detect":428,
    "top_splice_detect":425
}


# COMMAND ----------

# Get tag functions 
def _get_row_index_collection_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_VEC_General_ShiftRegister_RowIndex_Collection_Actual_0'

def _get_infeed_chute_tag_name(tt,id,machine,line):
    return tags_prefix + line + '_' + machine + '_Primary_BucketID_AtInfeedChute'

def _get_total_travel_time_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_VEC_General_Production_Product_TotalTravelTime_Actual_s'

def _get_packml_execute_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_PackML_Execute'

def _get_current_machine_speed_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_Current_Machine_Speed'

def _get_machine_speed_setpoint_tag_name(tt,id,machine,line):
    return tags_prefix + line + '_Machine_Speed_Setpoint'

def _get_distance_accumulator_actual_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_VEC_General_Master_0_DistanceAccumulator_Actual_mm'

def _get_machine_pitch_actual_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_VEC_General_Master_0_MachinePitch_Actual_mm'

def _get_bottom_shell_distance_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_VEC_Sealing_Bottom_NectarShell_Age_Actual_km'

def _get_splice_tag_name(self, ctx, unwinder):
    return self.tags_prefix + ctx.line + '_' + unwinder + '_Unwinder_SpliceInProgress'

def _get_middle_unwind_left_splice_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_middleUnwindleftspliceactive'

def _get_unwind_left_splice_tag_name(self, ctx, unwinder):
    return self.tags_prefix + ctx.line + '_' + unwinder + 'Unwindleftspliceactive'

def _get_unwind_right_splice_tag_name(self, ctx, unwinder):
    return self.tags_prefix + ctx.line + '_' + unwinder + 'Unwindrightspliceactive'

def _get_unwinder_speed_right_tag_name(self, ctx, unwinder):
    return self.tags_prefix + ctx.line + '_' + unwinder + '_Unwinder_Rotary_Speed_Right'

def _get_unwinder_speed_left_tag_name(self, ctx, unwinder):
    return self.tags_prefix + ctx.line + '_' + unwinder + '_Unwinder_Rotary_Speed_Left'

def _get_unwinder_diameter_tag_name(self, ctx, unwinder, current_roll_side):
    return self.tags_prefix + ctx.line + '_' + unwinder + '_Unwinder_Current_Diameter_' + current_roll_side

def _get_pva_gcas_tag_name(self, ctx, unwinder):
    return self.tags_prefix + ctx.line + '_VEC_WebHandling_' + unwinder + '_PVA_GCAS_Actual_#'

def _get_current_pva_roll_tag_name(self, ctx, unwinder):
    return self.tags_prefix + ctx.line + '_VEC_WebHandling_' + unwinder + '_PVA_Roll_Actual_#'

def _get_diverter_position_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_Diverter_Position'

def _get_product_at_diverter_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_VEC_Distribution_CPI_ProductAtDiverter_TimerStatus_State_#'

def _get_blocked_product_state_at_diverter_tag_name(self, ctx):
    return self.tags_prefix + ctx.line + '_VEC_Distribution_CPI_ProductAtDiverter_DelayedTravelTime_State_0'


# COMMAND ----------

from pyspark.sql import Window

def _get_infeed_chute_timestamp(tt, id, machine, line):
    tag_name = _get_infeed_chute_tag_name(tt, id, machine, line)
    buckets_per_current_line = 834
    max_missing_buckets = 5

    df_infeed_chute = (
        df_historian.filter(
            (F.col("TagName") == tag_name) & (F.col("Samples_Timestamp") < tt)
        )
        .sort(F.col("Samples_Timestamp").desc())
        .dropDuplicates(subset=["Value_Double"])
    )

    df_infeed_chute.cache().count()
    # df_infeed_chute.explain() TODO: Clean-up 

    # Find the range of possible bucket IDs to search for
    if (id + max_missing_buckets) <= (buckets_per_current_line - 1):
        nearest_max_bucket_id = id + max_missing_buckets
        min_bucket_id = id
        max_bucket_id = nearest_max_bucket_id
    else:
        nearest_max_bucket_id = max_missing_buckets - (buckets_per_current_line - id)
        min_bucket_id = id if id >= 0 else 0
        max_bucket_id = buckets_per_current_line - 1

    # Filter Historian data for relevant bucket IDs
    df_matching_or_nearest_bucket = df_infeed_chute.filter(
        (
            (F.col("Value_Double").between(min_bucket_id, max_bucket_id))
            | (F.col("Value_Double").between(0, nearest_max_bucket_id))
        )
    ).orderBy(F.col("Value_Double").desc())

    # df_matching_or_nearest_bucket.explain() # TODO: Clean-up

    # Find the timestamp closest to the rejected bucket's timestamp
    first_matching_bucket = df_matching_or_nearest_bucket.first()
    first_matching_timestamp = first_matching_bucket["Samples_Timestamp"]
    first_matching_bucket_id = first_matching_bucket["Value_Double"]

    timestamp_to_keep = first_matching_timestamp
    bucket_id_to_keep = first_matching_bucket_id

    # 
    for row in df_matching_or_nearest_bucket.collect():
        bucket_id = row["Value_Double"]
        potential_bucket_timestamp = (
            df_infeed_chute.filter(F.col("Value_Double") == bucket_id)
            .select("Samples_Timestamp")
            .first()["Samples_Timestamp"]
        )
        potential_bucket_id = (
            df_infeed_chute.filter(F.col("Value_Double") == bucket_id)
            .select("Value_Double")
            .first()["Value_Double"]
        )

        if potential_bucket_timestamp - first_matching_timestamp > timedelta(minutes=5):
            timestamp_to_keep = potential_bucket_timestamp
            bucket_id_to_keep = potential_bucket_id
            break

    return timestamp_to_keep

# COMMAND ----------

 
# Fn2
#  _get_machine_speed_setpoint  
def _get_machine_speed_setpoint(tt,id,machine,line,tt1):
    tag_name = _get_machine_speed_setpoint_tag_name(tt,id,machine,line)
    
    df_machine_speed_at_infeed = df_historian.filter(
        (F.col('TagName') == tag_name) & 
        (F.col('Value_Double') > 0) & 
        (F.col('Samples_Timestamp') <= tt1 )
    ).orderBy('Samples_Timestamp', ascending=False)

    machine_speed_at_infeed = df_machine_speed_at_infeed.select('Value_Double').first()[0]

    return machine_speed_at_infeed

# COMMAND ----------

# #Fn3 - _get_conv_all_events_timestamp
def _compute_travel_time(self, operation: str, machine_speed: float) -> timedelta:
    # Given a requested event or operation, this method will calculate the time difference between the in-feed chute and the requested event

    belt_1 = converter_constants['belt_1']
    belt_2 = converter_constants['belt_2']
    belt_3 = converter_constants['belt_3']
    z_pitch = converter_constants['z_pitch']
    z_num_lanes = converter_constants['z_num_lanes']
    belt_draw_percent = converter_constants['belt_draw_percent']
    d_pouch_drop_time = converter_constants['d_pouch_drop_time_in_s']
    reject_drop_off = converter_constants['reject_drop_off']

    operation_row_index = converter_op_row_index[operation]
    reject_valves_row_index = converter_op_row_index['reject_valves']

    #Calculations
    delta_row = reject_valves_row_index + reject_drop_off - operation_row_index
    belt_function = (belt_1+belt_2+belt_3)/(((100+belt_draw_percent)/100) * z_pitch)
    speed_factor = z_num_lanes/machine_speed
    travel_time = (delta_row + belt_function) * speed_factor + d_pouch_drop_time/60
    
    return timedelta(minutes=travel_time)

def _get_conv_all_events_timestamp(infeed_chute_timestamp, machine_speed_at_infeed):
    for key in converter_op_row_index:
        # 1. Compute travel time .
        travel_time = _compute_travel_time(key, machine_speed_at_infeed)
        #logger.info(f"Converter Travel Time for event: {key} is {travel_time}")
        
        # 2. Compute absolute timestamp by subtracting the time difference from in-feed chute timestamp
        absolute_time = infeed_chute_timestamp - travel_time
        
        # 3. Populate event in the context_event
        event_name = key + "_timestamp"
        
        return event_name
        #setattr(ctx.contextualized_events, event_name, absolute_time)

