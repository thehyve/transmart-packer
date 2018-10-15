from .patient_diagnosis_biosource_biomaterial import from_obs_json_to_pdbb_df

registry = {
    'reformat_export': from_obs_json_to_pdbb_df,
}
