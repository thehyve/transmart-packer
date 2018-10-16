import unittest
from packer.table_transformations.patient_diagnosis_biosource_biomaterial import \
    from_obs_df_to_pdbb_df
import numpy as np
import pandas as pd
import pandas.testing as pdt


class PatientDiagnosisBiosourceBiomaterialTranformations(unittest.TestCase):

    def test_result_data_shape_basic_with_sorting(self):
        self.test_data = [
            [np.nan, np.nan, np.nan, 'patient_concept_1', '\\01.Patient\\Age\\', 'Age', 42.0, 1, 'P1', np.nan, 'TEST'],
            [np.nan, np.nan, 'D1', 'diagnosis_concept_1', '\\02.Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 1, 'P1', 'Diagnosis 1 Name', 'TEST'],
            [np.nan, 'BS1', 'D1', 'biosource_concept_1', '\\03.Biosource\\Cell type\\', 'Cell type',
             np.nan, 1, 'P1', 'Skin', 'TEST'],
            ['BM1', 'BS1', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 1, 'P1', 'Tue Apr 24 02:00:00 CEST 2018', 'TEST'],
            ['BM2', 'BS1', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 1, 'P1', 'Wed Mar 07 01:00:00 CET 2018', 'TEST'],
            [np.nan, np.nan, np.nan, 'patient_concept_1', '\\01.Patient\\Age\\', 'Age', 39.0, 2, 'P2', np.nan, 'TEST'],
            [np.nan, np.nan, 'D1', 'diagnosis_concept_1', '\\02.Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 2, 'P2', 'Diagnosis 1 Name 2', 'TEST'],
            [np.nan, 'BS2', 'D1', 'biosource_concept_1', '\\03.Biosource\\Cell type\\', 'Cell type',
             np.nan, 2, 'P2', 'Liver', 'TEST'],
            ['BM3', 'BS2', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 2, 'P2', 'Fri Jan 19 01:00:00 CET 2018', 'TEST'],
            ['BM4', 'BS2', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 2, 'P2', 'Sun Jun 05 02:00:00 CEST 2011', 'TEST']]
        observations_df = pd.DataFrame(self.test_data, columns=['PMC Biomaterial ID', 'PMC Biosource ID',
                                                                'PMC Diagnosis ID', 'concept.conceptCode',
                                                                'concept.conceptPath', 'concept.name',
                                                                'numericValue', 'patient.id', 'patient.trial',
                                                                'stringValue', 'study.name'])

        df = from_obs_df_to_pdbb_df(observations_df)

        self.assertIsNotNone(df)
        pdt.assert_frame_equal(df, pd.DataFrame([
            ['P1', 'D1', 'BS1', 'BM1', 42, 'Diagnosis 1 Name', 'Skin', 'Tue Apr 24 02:00:00 CEST 2018'],
            ['P1', 'D1', 'BS1', 'BM2', 42, 'Diagnosis 1 Name', 'Skin', 'Wed Mar 07 01:00:00 CET 2018'],
            ['P2', 'D1', 'BS2', 'BM3', 39, 'Diagnosis 1 Name 2', 'Liver', 'Fri Jan 19 01:00:00 CET 2018'],
            ['P2', 'D1', 'BS2', 'BM4', 39, 'Diagnosis 1 Name 2', 'Liver', 'Sun Jun 05 02:00:00 CEST 2011'],
        ], columns=['Patient Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id',
                    'Age', 'Diagnosis Name', 'Cell type', 'Date of biomaterial']))

    def test_result_data_shape_no_biomaterial_column(self):
        self.test_data = [
            [np.nan, np.nan, 'patient_concept_1', '\\Patient\\Age\\', 'Age', 42.0, 1, 'P1', np.nan, 'TEST'],
            [np.nan, 'D1', 'diagnosis_concept_1', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 1, 'P1', 'Diagnosis 1 Name', 'TEST'],
            ['BS1', 'D1', 'biosource_concept_1', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             np.nan, 1, 'P1', 'Skin', 'TEST'],
            [np.nan, np.nan, 'patient_concept_1', '\\Patient\\Age\\', 'Age', 39.0, 2, 'P2', np.nan, 'TEST'],
            [np.nan, 'D1', 'diagnosis_concept_1', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 2, 'P2', 'Diagnosis 1 Name 2', 'TEST'],
            ['BS2', 'D1', 'biosource_concept_1', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             np.nan, 2, 'P2', 'Liver', 'TEST']
            ]
        observations_df = pd.DataFrame(self.test_data, columns=['PMC Biosource ID', 'PMC Diagnosis ID',
                                                                'concept.conceptCode', 'concept.conceptPath',
                                                                'concept.name', 'numericValue', 'patient.id',
                                                                'patient.trial', 'stringValue', 'study.name'])

        df = from_obs_df_to_pdbb_df(observations_df)

        self.assertIsNotNone(df)
        pdt.assert_frame_equal(df, pd.DataFrame([
            ['P1', 'D1', 'BS1', 42, 'Skin', 'Diagnosis 1 Name'],
            ['P2', 'D1', 'BS2', 39, 'Liver', 'Diagnosis 1 Name 2'],
        ], columns=['Patient Id', 'Diagnosis Id', 'Biosource Id',
                    'Age', 'Cell type', 'Diagnosis Name']))

    def test_result_data_shape_no_biosource_no_biomaterial_columns(self):
        self.test_data = [
            [np.nan, 'patient_concept_1', '\\Patient\\Age\\', 'Age', 42.0, 1, 'P1', np.nan, 'TEST'],
            ['D1', 'diagnosis_concept_1', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 1, 'P1', 'Diagnosis 1 Name', 'TEST'],
            [np.nan, 'patient_concept_1', '\\Patient\\Age\\', 'Age', 39.0, 2, 'P2', np.nan, 'TEST'],
            ['D1', 'diagnosis_concept_1', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             np.nan, 2, 'P2', 'Diagnosis 1 Name 2', 'TEST'],
            ]
        observations_df = pd.DataFrame(self.test_data, columns=['PMC Diagnosis ID',
                                                                'concept.conceptCode', 'concept.conceptPath',
                                                                'concept.name', 'numericValue', 'patient.id',
                                                                'patient.trial', 'stringValue', 'study.name'])

        df = from_obs_df_to_pdbb_df(observations_df)

        self.assertIsNotNone(df)
        pdt.assert_frame_equal(df, pd.DataFrame([
            ['P1', 'D1', 42, 'Diagnosis 1 Name'],
            ['P2', 'D1', 39, 'Diagnosis 1 Name 2'],
        ], columns=['Patient Id', 'Diagnosis Id',
                    'Age', 'Diagnosis Name']))

    def test_result_data_shape_patient_column_only(self):
        self.test_data = [
            ['patient_concept_1', '\\Patient\\Age\\', 'Age', 42.0, 1, 'P1', np.nan, 'TEST'],
            ['patient_concept_1', '\\Patient\\Age\\', 'Age', 39.0, 2, 'P2', np.nan, 'TEST'],
            ]
        observations_df = pd.DataFrame(self.test_data, columns=['concept.conceptCode', 'concept.conceptPath',
                                                                'concept.name', 'numericValue', 'patient.id',
                                                                'patient.trial', 'stringValue', 'study.name'])

        df = from_obs_df_to_pdbb_df(observations_df)

        self.assertIsNotNone(df)
        pdt.assert_frame_equal(df, pd.DataFrame([
            ['P1', 42],
            ['P2', 39],
        ], columns=['Patient Id',
                    'Age']))

    def test_result_data_shape_no_diagnosis_observations_with_sorting(self):
        self.test_data = [
            [np.nan, np.nan, np.nan, 'patient_concept_1', '\\01.Patient\\Age\\', 'Age', 42.0, 1, 'P1', np.nan, 'TEST'],
            [np.nan, 'BS1', 'D1', 'biosource_concept_1', '\\03.Biosource\\Cell type\\', 'Cell type',
             np.nan, 1, 'P1', 'Skin', 'TEST'],
            ['BM1', 'BS1', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 1, 'P1', 'Tue Apr 24 02:00:00 CEST 2018', 'TEST'],
            ['BM2', 'BS1', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 1, 'P1', 'Wed Mar 07 01:00:00 CET 2018', 'TEST'],
            [np.nan, np.nan, np.nan, 'patient_concept_1', '\\01.Patient\\Age\\', 'Age', 39.0, 2, 'P2', np.nan, 'TEST'],
            [np.nan, 'BS2', 'D1', 'biosource_concept_1', '\\03.Biosource\\Cell type\\', 'Cell type',
             np.nan, 2, 'P2', 'Liver', 'TEST'],
            ['BM3', 'BS2', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 2, 'P2', 'Fri Jan 19 01:00:00 CET 2018', 'TEST'],
            ['BM4', 'BS2', 'D1', 'biomaterial_concept_1', '\\04.Biomaterial\\Date\\',
             'Date of biomaterial', np.nan, 2, 'P2', 'Sun Jun 05 02:00:00 CEST 2011', 'TEST']]
        observations_df = pd.DataFrame(self.test_data, columns=['PMC Biomaterial ID', 'PMC Biosource ID',
                                                                'PMC Diagnosis ID', 'concept.conceptCode',
                                                                'concept.conceptPath', 'concept.name',
                                                                'numericValue', 'patient.id', 'patient.trial',
                                                                'stringValue', 'study.name'])

        df = from_obs_df_to_pdbb_df(observations_df)

        self.assertIsNotNone(df)
        pdt.assert_frame_equal(df, pd.DataFrame([
            ['P1', 'D1', 'BS1', 'BM1', 42, 'Skin', 'Tue Apr 24 02:00:00 CEST 2018'],
            ['P1', 'D1', 'BS1', 'BM2', 42, 'Skin', 'Wed Mar 07 01:00:00 CET 2018'],
            ['P2', 'D1', 'BS2', 'BM3', 39, 'Liver', 'Fri Jan 19 01:00:00 CET 2018'],
            ['P2', 'D1', 'BS2', 'BM4', 39, 'Liver', 'Sun Jun 05 02:00:00 CEST 2011'],
        ], columns=['Patient Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id',
                    'Age', 'Cell type', 'Date of biomaterial']))

    def test_empty_data(self):
        test_obs = pd.DataFrame()
        result_obs = from_obs_df_to_pdbb_df(test_obs)
        self.assertIsNotNone(result_obs)
        self.assertTrue(result_obs.empty)
        self.assertEqual(result_obs.size, 0)


if __name__ == '__main__':
    unittest.main()
