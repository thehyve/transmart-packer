import unittest
from packer.table_transformations.patient_diagnosis_biosource_biomaterial import \
    from_obs_json_to_pdbb_df, from_obs_df_to_pdbb_df
import numpy as np
import pandas as pd
import pandas.testing as pdt


class PatientDiagnosisBiosourceBiomaterialTranformations(unittest.TestCase):

    def test_no_observations(self):
        observations_api_response = {
            'dimensionDeclarations': [],
            'cells': [],
            'dimensionElements': {}
        }

        df = from_obs_json_to_pdbb_df(observations_api_response)

        self.assertIsNotNone(df)
        self.assertEqual(df.size, 0)

    def test_result_data_shape(self):
        self.test_data = [
            [np.nan, np.nan, np.nan, 'patient_concept_1', '\\Patient\\Age\\', 'Age', 42.0, 1, 'P1', np.nan, 'TEST'],
            [np.nan, np.nan, 'D1', 'diagnosis_concept_1', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 1, 'P1', 'Diagnosis 1 Name', 'TEST'],
            [np.nan, 'BS1', 'D1', 'biosource_concept_1', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             np.nan, 1, 'P1', 'Skin', 'TEST'],
            ['BM1', 'BS1', 'D1', 'biomaterial_concept_1', '\\Patient\\Diagnosis\\Biosource\\Biomaterial\Date\\',
             'Date of biomaterial', np.nan, 1, 'P1', 'Tue Apr 24 02:00:00 CEST 2018', 'TEST'],
            ['BM2', 'BS1', 'D1', 'biomaterial_concept_1', '\\Patient\\Diagnosis\\Biosource\\Biomaterial\Date\\',
             'Date of biomaterial', np.nan, 1, 'P1', 'Wed Mar 07 01:00:00 CET 2018', 'TEST'],
            [np.nan, np.nan, np.nan, 'patient_concept_1', '\\Patient\\Age\\', 'Age', 39.0, 2, 'P2', np.nan, 'TEST'],
            [np.nan, np.nan, 'D1', 'diagnosis_concept_1', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 2, 'P2', 'Diagnosis 1 Name 2', 'TEST'],
            [np.nan, 'BS2', 'D1', 'biosource_concept_1', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             np.nan, 2, 'P2', 'Liver', 'TEST'],
            ['BM3', 'BS2', 'D1', 'biomaterial_concept_1', '\\Patient\\Diagnosis\\Biosource\\Biomaterial\Date\\',
             'Date of biomaterial', np.nan, 2, 'P2', 'Fri Jan 19 01:00:00 CET 2018', 'TEST'],
            ['BM4', 'BS2', 'D1', 'biomaterial_concept_1', '\\Patient\\Diagnosis\\Biosource\\Biomaterial\Date\\',
             'Date of biomaterial', np.nan, 2, 'P2', 'Sun Jun 05 02:00:00 CEST 2011', 'TEST']]
        observations_df = pd.DataFrame(self.test_data, columns=['PMC Biomaterial ID', 'PMC Biosource ID',
                                                                'PMC Diagnosis ID', 'concept.conceptCode',
                                                                'concept.conceptPath', 'concept.name',
                                                                'numericValue', 'patient.id', 'patient.trial',
                                                                'stringValue', 'study.name'])

        df = from_obs_df_to_pdbb_df(observations_df)

        self.assertIsNotNone(df)
        pdt.assert_frame_equal(df, pd.DataFrame([
            ['P1', 'D1', 'BS1', 'BM1', 42.0, 'Tue Apr 24 02:00:00 CEST 2018', 'Skin', 'Diagnosis 1 Name'],
            ['P1', 'D1', 'BS1', 'BM2', 42.0, 'Wed Mar 07 01:00:00 CET 2018', 'Skin', 'Diagnosis 1 Name'],
            ['P2', 'D1', 'BS2', 'BM3', 39.0, 'Fri Jan 19 01:00:00 CET 2018', 'Liver', 'Diagnosis 1 Name 2'],
            ['P2', 'D1', 'BS2', 'BM4', 39.0, 'Sun Jun 05 02:00:00 CEST 2011', 'Liver', 'Diagnosis 1 Name 2'],
        ], columns=['Patient ID', 'PMC Diagnosis ID', 'PMC Biosource ID',
                                                             'PMC Biomaterial ID', 'Age', 'Date of biomaterial',
                                                             'Cell type', 'Diagnosis Name']))

    def test_empty_export_reformatting(self):
        test_obs = pd.DataFrame()
        self.assertTrue(from_obs_df_to_pdbb_df(test_obs).empty)


if __name__ == '__main__':
    unittest.main()
