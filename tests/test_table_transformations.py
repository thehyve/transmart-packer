import unittest
from packer.table_transformations.csr_transformations import \
    from_obs_df_to_csr_df, format_columns, from_obs_json_to_export_csr_df, transform_obs_df
import pandas as pd
import pandas.testing as pdt
import os
import json


class CsrTranformations(unittest.TestCase):

    def test_result_data_shape_basic_with_sorting(self):
        test_data = [
            [None, None, None, 'Individual.age', '\\01.Patient\\Age\\', 'Age', 42.0, 1, 'P1', None, 'TEST'],
            [None, None, 'D1', 'Diagnosis.age', '\\02.Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             None, 1, 'P1', 'Diagnosis 1 Name', 'TEST'],
            [None, None, 'D2', 'Diagnosis.name', '\\02.Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             None, 1, 'P1', 'Diagnosis 2 Name', 'TEST'],
            [None, 'BS1', 'D1', 'Biosource.cell_type', '\\03.Biosource\\Cell type\\', 'Cell type',
             None, 1, 'P1', 'Skin', 'TEST'],
            ['BM1', 'BS1', 'D1', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 1, 'P1', '2018-04-24T02:00:00Z', 'TEST'],
            [None, 'BS2', 'D2', 'Biosource.cell_type', '\\03.Biosource\\Cell type\\', 'Cell type',
             None, 1, 'P1', 'Tissue 2', 'TEST'],
            ['BM2', 'BS2', 'D2', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 1, 'P1', 'Wed Mar 07 01:00:00 CET 2018', 'TEST'],
            [None, None, None, 'Individual.age', '\\01.Patient\\Age\\', 'Age', 39.0, 2, 'P2', None, 'TEST'],
            [None, None, 'D3', 'Diagnosis.name', '\\02.Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             None, 2, 'P2', 'Diagnosis 3 Name', 'TEST'],
            [None, 'BS3', 'D3', 'Biosource.cell_type', '\\03.Biosource\\Cell type\\', 'Cell type',
             None, 2, 'P2', 'Liver', 'TEST'],
            ['BM3', 'BS3', 'D3', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 2, 'P2', 'Fri Jan 19 01:00:00 CET 2018', 'TEST'],
            ['BM4', 'BS3', 'D3', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 2, 'P2', 'Sun Jun 05 02:00:00 CEST 2011', 'TEST']]
        observations_df = pd.DataFrame(test_data, columns=['Biomaterial', 'Biosource',
                                                                'Diagnosis', 'concept.conceptCode',
                                                                'concept.conceptPath', 'concept.name',
                                                                'numericValue',
                                                                'patient.id', 'patient.subjectIds.SUBJ_ID',
                                                                'stringValue', 'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'D1', 'BS1', 'BM1', 42., 'Diagnosis 1 Name', 'Skin', '2018-04-24T02:00:00Z'],
            ['P1', 'D2', 'BS2', 'BM2', 42., 'Diagnosis 2 Name', 'Tissue 2', 'Wed Mar 07 01:00:00 CET 2018'],
            ['P2', 'D3', 'BS3', 'BM3', 39., 'Diagnosis 3 Name', 'Liver', 'Fri Jan 19 01:00:00 CET 2018'],
            ['P2', 'D3', 'BS3', 'BM4', 39., 'Diagnosis 3 Name', 'Liver', 'Sun Jun 05 02:00:00 CEST 2011'],
        ], columns=['Subject Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id',
                    '\\01.Patient\\Age\\', '\\02.Diagnosis\\Diagnosis Name\\', '\\03.Biosource\\Cell type\\',
                    '\\04.Biomaterial\\Date\\'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)


    def test_result_data_shape_no_biomaterial_column(self):
        test_data = [
            [None, None, 'Individual.age', '\\Patient\\Age\\', 'Age', 42.0, 1, 'P1', None, 'TEST'],
            [None, 'D1', 'Diagnosis.name', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             None, 1, 'P1', 'Diagnosis 1 Name', 'TEST'],
            ['BS1', 'D1', 'Biosource.cell_type', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             None, 1, 'P1', 'Skin', 'TEST'],
            [None, None, 'Individual.age', '\\Patient\\Age\\', 'Age', 39.0, 2, 'P2', None, 'TEST'],
            [None, 'D2', 'Diagnosis.name', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             None, 2, 'P2', 'Diagnosis 2 Name', 'TEST'],
            ['BS2', 'D2', 'Biosource.cell_type', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             None, 2, 'P2', 'Liver', 'TEST']
            ]
        observations_df = pd.DataFrame(test_data, columns=['Biosource', 'Diagnosis',
                                                           'concept.conceptCode', 'concept.conceptPath',
                                                           'concept.name', 'numericValue',
                                                           'patient.id', 'patient.subjectIds.SUBJ_ID',
                                                           'stringValue', 'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'D1', 'BS1', 42., 'Diagnosis 1 Name', 'Skin'],
            ['P2', 'D2', 'BS2', 39., 'Diagnosis 2 Name', 'Liver'],
        ], columns=['Subject Id', 'Diagnosis Id', 'Biosource Id',
                    '\\Patient\\Age\\', '\\Patient\\Diagnosis\\Diagnosis Name\\',
                    '\\Patient\\Diagnosis\\Biosource\\Cell type\\'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id', 'Biosource Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)

    def test_result_data_shape_no_biosource_no_biomaterial_columns(self):
        test_data = [
            [None, 'Individual.age', '\\Patient\\Age\\', 'Age', 42.0, 1, 'P1', None, 'TEST'],
            ['D1', 'Diagnosis.name', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             None, 1, 'P1', 'Diagnosis 1 Name', 'TEST'],
            [None, 'Individual.age', '\\Patient\\Age\\', 'Age', 39.0, 2, 'P2', None, 'TEST'],
            ['D2', 'Diagnosis.name', '\\Patient\\Diagnosis\\Diagnosis Name\\', 'Diagnosis Name',
             None, 2, 'P2', 'Diagnosis 2 Name', 'TEST'],
            ]
        observations_df = pd.DataFrame(test_data, columns=['Diagnosis',
                                                           'concept.conceptCode', 'concept.conceptPath',
                                                           'concept.name', 'numericValue',
                                                           'patient.id', 'patient.subjectIds.SUBJ_ID',
                                                           'stringValue', 'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'D1', 42., 'Diagnosis 1 Name'],
            ['P2', 'D2', 39., 'Diagnosis 2 Name'],
        ], columns=['Subject Id', 'Diagnosis Id',
                    '\\Patient\\Age\\', '\\Patient\\Diagnosis\\Diagnosis Name\\'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)

    def test_result_data_shape_patient_column_only(self):
        test_data = [
            ['Individual.age', '\\Patient\\Age\\', 'Age', 42.0, 1, 'P1', None, 'TEST'],
            ['Individual.age', '\\Patient\\Age\\', 'Age', 39.0, 2, 'P2', None, 'TEST']
        ]
        observations_df = pd.DataFrame(test_data, columns=['concept.conceptCode', 'concept.conceptPath',
                                                           'concept.name', 'numericValue',
                                                           'patient.id', 'patient.subjectIds.SUBJ_ID',
                                                           'stringValue', 'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 42.],
            ['P2', 39.],
        ], columns=['Subject Id',
                    '\\Patient\\Age\\'])
        expected_df.set_index(['Subject Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)

    def test_result_data_shape_no_diagnosis_observations_with_sorting(self):
        test_data = [
            [None, None, None, 'Individual.age', '\\01.Patient\\Age\\', 'Age', 42.0, 1, 'P1', None, 'TEST'],
            [None, 'BS1', 'D1', 'Biosource.cell_type', '\\03.Biosource\\Cell type\\', 'Cell type',
             None, 1, 'P1', 'Skin', 'TEST'],
            ['BM1', 'BS1', 'D1', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 1, 'P1', '2018-04-24T02:00:00Z', 'TEST'],
            ['BM2', 'BS1', 'D1', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 1, 'P1', '2018-03-07T01:00:00Z', 'TEST'],
            [None, None, None, 'Individual.age', '\\01.Patient\\Age\\', 'Age', 39.0, 2, 'P2', None, 'TEST'],
            [None, 'BS2', 'D2', 'Biosource.cell_type', '\\03.Biosource\\Cell type\\', 'Cell type',
             None, 2, 'P2', 'Liver', 'TEST'],
            ['BM3', 'BS2', 'D2', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 2, 'P2', '2018-01-19T01:00:00Z', 'TEST'],
            ['BM4', 'BS2', 'D2', 'Biomaterial.date', '\\04.Biomaterial\\Date\\',
             '01. Date of biomaterial', None, 2, 'P2', '2011-06-05T02:00:00Z', 'TEST']]
        observations_df = pd.DataFrame(test_data, columns=['Biomaterial', 'Biosource',
                                                           'Diagnosis', 'concept.conceptCode',
                                                           'concept.conceptPath', 'concept.name',
                                                           'numericValue',
                                                           'patient.id', 'patient.subjectIds.SUBJ_ID',
                                                           'stringValue', 'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'D1', 'BS1', 'BM1', 42., 'Skin', '2018-04-24T02:00:00Z'],
            ['P1', 'D1', 'BS1', 'BM2', 42., 'Skin', '2018-03-07T01:00:00Z'],
            ['P2', 'D2', 'BS2', 'BM3', 39., 'Liver', '2018-01-19T01:00:00Z'],
            ['P2', 'D2', 'BS2', 'BM4', 39., 'Liver', '2011-06-05T02:00:00Z']
        ], columns=['Subject Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id',
                    '\\01.Patient\\Age\\', '\\03.Biosource\\Cell type\\', '\\04.Biomaterial\\Date\\'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)

    def test_empty_data(self):
        test_obs = pd.DataFrame()
        result_obs = from_obs_df_to_csr_df(test_obs)
        self.assertIsNotNone(result_obs)
        self.assertTrue(result_obs.empty)
        self.assertEqual(result_obs.size, 0)

    def test_concept_path_respected(self):
        test_data = [
            ['Diagnosis.name', '\\02. Diagnosis\\Name\\', 'Name', None, 1, 'P1', 'Diagnosis 1', 'TEST'],
            ['Biosource.name', '\\03. Biosource\\Name\\', 'Name', None, 1, 'P1', 'Biosource 1', 'TEST'],
            ['Individual.name', '\\01. Patient\\Name\\', 'Name', None, 1, 'P1', 'Patient 1', 'TEST'],
            ['Biomaterial.name', '\\04. Biomaterial\\Name\\', 'Name', None, 1, 'P1', 'Biomaterial 1', 'TEST'],
            ['Individual.name', '\\01. Patient\\Name\\', 'Name', None, 2, 'P2', 'Patient 2', 'TEST']
        ]
        observations_df = pd.DataFrame(test_data, columns=['concept.conceptCode', 'concept.conceptPath',
                                                           'concept.name', 'numericValue', 'patient.id',
                                                           'patient.subjectIds.SUBJ_ID', 'stringValue',
                                                           'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'Patient 1', 'Diagnosis 1', 'Biosource 1', 'Biomaterial 1'],
            ['P2', 'Patient 2', None, None, None]
        ], columns=['Subject Id',
                    '\\01. Patient\\Name\\', '\\02. Diagnosis\\Name\\', '\\03. Biosource\\Name\\',
                    '\\04. Biomaterial\\Name\\'])
        expected_df.set_index(['Subject Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)

    def test_values_propagation(self):
        test_data = [
            [1, 'P1', None, None, None, 'Individual.text', '\\01. Patient\\Text\\', 'Text', None, 'Patient #1', 'T'],
            [1, 'P1', None, None, None, 'Individual.number', '\\01. Patient\\Number\\', 'Number', 5., None, 'T'],
            [1, 'P1', 'D1', None, None, 'Diagnosis.text', '\\02. Diagnosis\\Text\\', 'Text', None, 'Diagnosis #1', 'T'],
            [1, 'P1', 'D2', None, None, 'Diagnosis.number', '\\02. Diagnosis\\Number\\', 'Number', 10., None, 'T'],
            [1, 'P1', 'D2', None, None, 'Diagnosis.text', '\\02. Diagnosis\\Text\\', 'Text', None, 'Diagnosis #2', 'T'],
            [1, 'P1', 'D1', 'BS1', None, 'Biosource.number', '\\03. Biosource\\Number\\', 'Number', 15., None, 'T'],
            [1, 'P1', 'D1', 'BS1', None, 'Biosource.text', '\\03. Biosource\\Text\\', 'Text', None, 'Biosource #1', 'T'],
            [1, 'P1', 'D2', 'BS2', None, 'Biosource.number', '\\03. Biosource\\Number\\', 'Number', 20., None, 'T'],
            [1, 'P1', 'D1', 'BS1', 'BM1', 'Biomaterial.number', '\\04. Biomaterial\\Number\\', 'Number', 25., None, 'T'],
            [1, 'P1', 'D1', 'BS1', 'BM1', 'Biomaterial.text', '\\04. Biomaterial\\Text\\', 'Text', None, 'Biomaterial #1', 'T'],
            [1, 'P1', 'D1', 'BS1', 'BM2', 'Biomaterial.text', '\\04. Biomaterial\\Text\\', 'Text', None, 'Biomaterial #2', 'T'],

            [2, 'P2', None, None, None, 'pnum', '\\01. Patient\\Number\\', 'Number', 30., None, 'T'],
            [2, 'P2', 'D3', None, None, 'dnum', '\\02. Diagnosis\\Number\\', 'Number', 35., None, 'T']
        ]
        observations_df = pd.DataFrame(test_data, columns=[
            'patient.id', 'patient.subjectIds.SUBJ_ID', 'Diagnosis', 'Biosource', 'Biomaterial',
            'concept.conceptCode', 'concept.conceptPath', 'concept.name', 'numericValue', 'stringValue', 'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'D1', 'BS1', 'BM1', 5., 'Patient #1', None, 'Diagnosis #1', 15., 'Biosource #1', 25.,
             'Biomaterial #1'],
            ['P1', 'D1', 'BS1', 'BM2', 5., 'Patient #1', None, 'Diagnosis #1', 15., 'Biosource #1', None,
             'Biomaterial #2'],
            ['P1', 'D2', 'BS2', '', 5., 'Patient #1', 10, 'Diagnosis #2', 20, None, None, None],
            ['P2', 'D3', '', '', 30., None, 35., None, None, None, None, None]
        ], columns=['Subject Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id',
                    '\\01. Patient\\Number\\', '\\01. Patient\\Text\\', '\\02. Diagnosis\\Number\\',
                    '\\02. Diagnosis\\Text\\',
                    '\\03. Biosource\\Number\\', '\\03. Biosource\\Text\\', '\\04. Biomaterial\\Number\\',
                    '\\04. Biomaterial\\Text\\'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id', 'Biosource Id', 'Biomaterial Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)

    def test_format_columns(self):
        src_df = pd.DataFrame(
            {
                '01. Date of smth': ['2018-04-24T02:00:00Z', 'Wed Mar 07 01:00:00 CET 2018', 'NA', None],
                '02. Number': [None, 30.0, 2.00001, 7.5],
                '03. Text': ['1.0', 'yes', '', 'Wed Mar 07 01:00:00 CET 2018']
            })

        frmt_df = format_columns(src_df)

        pdt.assert_frame_equal(frmt_df, pd.DataFrame(
            {
                '01. Date of smth': ['2018-04-24', '2018-03-07', 'NA', ''],
                '02. Number': ['', '30', '2.00001', '7.5'],
                '03. Text': ['1.0', 'yes', '', 'Wed Mar 07 01:00:00 CET 2018'],
            }))

    def test_format_columns_with_the_same_name(self):
        src_df = pd.DataFrame(
            [
                [1., None],
                [None, 2.],
            ], columns=['Number', 'Number'])

        frmt_df = format_columns(src_df)

        pdt.assert_frame_equal(frmt_df, pd.DataFrame(
            [
                ['1', ''],
                ['', '2']
            ], columns=['Number', 'Number']))

    def test_diagnoseless_biosources(self):
        test_data = [
            ['BS1', 'D1', 'Biosource.cell_type', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             None, 1, 'P1', 'Skin', 'TEST'],
            ['BS2', None, 'Biosource.cell_type', '\\Patient\\Diagnosis\\Biosource\\Cell type\\', 'Cell type',
             None, 2, 'P2', 'Liver', 'TEST']
            ]
        observations_df = pd.DataFrame(test_data, columns=['Biosource', 'Diagnosis',
                                                           'concept.conceptCode', 'concept.conceptPath',
                                                           'concept.name', 'numericValue',
                                                           'patient.id', 'patient.subjectIds.SUBJ_ID',
                                                           'stringValue', 'study.name'])

        df = from_obs_df_to_csr_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'D1', 'BS1', 'Skin'],
            ['P2', '', 'BS2', 'Liver'],
        ], columns=['Subject Id', 'Diagnosis Id', 'Biosource Id',
                    '\\Patient\\Diagnosis\\Biosource\\Cell type\\'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id', 'Biosource Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df)

    def test_result_data_shape_patient_radiology(self):
        test_data = [
            ['P1', None, 'Individual.gender', '\\01.Subject\\Gender\\', 'Gender', 'Female', None, 1, 'TEST'],
            ['P2', None, 'Individual.gender', '\\01.Subject\\Gender\\', 'Gender', 'Male', None, 1, 'TEST'],
            ['P1', 'R1', 'Radiology.image_type', '\\03.Radiology\\Image type\\', 'Image type', None, 'type_1', 1, 'TEST'],
            ['P1', 'R1', 'Radiology.body_part', '\\03.Radiology\\Body part\\', 'Body part', None, 'torso', 1, 'TEST'],
            ['P1', 'R2', 'Radiology.body_part', '\\03.Radiology\\Body part\\', 'Body part', None, 'legs', 1, 'TEST'],
            ['P2', 'R3', 'Radiology.body_part', '\\03.Radiology\\Image type\\', 'Image type', None, 'type_1', 1, 'TEST']
        ]
        observations_df = pd.DataFrame(test_data, columns=['patient.subjectIds.SUBJ_ID',
                                                                'Radiology',
                                                                'concept.conceptCode', 'concept.conceptPath',
                                                                'concept.name', 'numericValue',
                                                                'stringValue', 'patient.id', 'study.name'])
        df = transform_obs_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'R1', 'Female', 'type_1', 'torso'],
            ['P1', 'R2', 'Female', '', 'legs'],
            ['P2', 'R3', 'Male', 'type_1', '']
        ], columns=['Subject Id', 'Radiology Id',
                    'Gender', 'Image type', 'Body part'])
        expected_df.set_index(['Subject Id', 'Radiology Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df, check_dtype=False, check_categorical=False, check_like=True)


    def test_result_data_shape_patient_diagnosis_radiology(self):
        test_data = [
            ['P1', None, None, 'Individual.gender', '\\01.Subject\\Gender\\', 'Gender', None, 'Female', 1, 'TEST'],
            ['P1', 'D1', None, 'Diagnosis.name', '\\02.Diagnosis\\Name\\', 'Diagnosis', None, 'Leukemia', 1, 'TEST'],
            ['P1', None, 'R1', 'Radiology.image_type', '\\03.Radiology\\Image type\\', 'Image type', None, 'type_1', 1, 'TEST']
        ]
        observations_df = pd.DataFrame(test_data, columns=['patient.subjectIds.SUBJ_ID', 'Diagnosis', 'Radiology',
                                                           'concept.conceptCode', 'concept.conceptPath',
                                                           'concept.name', 'numericValue',
                                                           'stringValue', 'patient.id', 'study.name'])

        df = transform_obs_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', '', 'R1', '', 'type_1', ''],
            ['P1', 'D1', '', 'Female', '', 'Leukemia'],
        ], columns=['Subject Id', 'Diagnosis Id', 'Radiology Id',
                    'Gender', 'Image type', 'Diagnosis'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id', 'Radiology Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df, check_dtype=False, check_categorical=False, check_like=True)


    def test_result_data_shape_patient_diagnosis_radiology_study(self):
        test_data = [
            ['P1', None, None, None, 'Individual.gender', '\\01.Subject\\Gender\\', 'Gender', 'Female', None, 1, 'TEST'],
            ['P1', 'D1', None, None, 'Diagnosis.name', '\\02.Diagnosis\\Name\\', 'Diagnosis', None, 'Leukemia', 1, 'TEST'],
            ['P1', 'D2', None, None, 'Diagnosis.name', '\\02.Diagnosis\\Name\\', 'Diagnosis', None, 'CRC', 1, 'TEST'],
            ['P1', 'D1', 'R1', None, 'Radiology.examination_date', '\\03.Radiology\\Examination Date\\', 'Radiology date', None, '2021-12-17T00:00:00Z', 1, 'TEST'],
            ['P1', 'D1', 'R2', None, 'Radiology.examination_date', '\\03.Radiology\\Examination Date\\', 'Radiology date', None, '2020-07-12T00:00:00Z', 1, 'TEST'],
            ['P1', None, 'R3', None, 'Radiology.examination_date', '\\03.Radiology\\Examination Date\\', 'Radiology date', None, '2021-12-24T00:00:00Z', 1, 'TEST'],
            ['P1', 'D2', 'R4', None, 'Radiology.examination_date', '\\03.Radiology\\Examination Date\\', 'Radiology date', None, '2021-12-23T00:00:00Z', 1, 'TEST'],
            ['P1', 'D1', None, None, 'Diagnosis.treatment_protocol', '\\02.Diagnosis\\treatment_protocol\\', 'Treatment', None, 'Chemo', 1, 'TEST'],
            ['P1', None, None, 'Study1', 'Study.title', '\\Study\\Title\\', 'Study title',  None, 'Study 1', 1, 'TEST'],
            ['P1', None, None, 'Study2', 'Study.title', '\\Study\\Title\\', 'Study title', None, 'Study 2', 1, 'TEST'],
            ['P1', None, None, 'Study1', 'IndividualStudy.individual_study_id', '\\IndividualStudy\\individual_study_id\\', 'Individual study id', None, '1', 1, 'TEST'],
            ['P1', None, None, 'Study2', 'IndividualStudy.individual_study_id', '\\IndividualStudy\\individual_study_id\\', 'Individual study id',None, '2', 1, 'TEST']
        ]
        observations_df = pd.DataFrame(test_data, columns=['patient.subjectIds.SUBJ_ID',
                                                           'Diagnosis', 'Radiology', 'Study',
                                                           'concept.conceptCode', 'concept.conceptPath',
                                                           'concept.name', 'numericValue',
                                                           'stringValue', 'patient.id', 'study.name'])

        df = transform_obs_df(observations_df)

        self.assertIsNotNone(df)
        expected_df = pd.DataFrame([
            ['P1', 'D1', 'R1', 'Study1',  'Female', 'Leukemia', 'Chemo', '2021-12-17', 'Study 1', '1'],
            ['P1', 'D1', 'R1', 'Study2', 'Female', 'Leukemia', 'Chemo', '2021-12-17', 'Study 2', '2'],
            ['P1', 'D1', 'R2', 'Study1',  'Female', 'Leukemia', 'Chemo', '2020-07-12', 'Study 1', '1'],
            ['P1', 'D1', 'R2', 'Study2', 'Female', 'Leukemia', 'Chemo', '2020-07-12', 'Study 2', '2'],
            ['P1', 'D2', 'R4', 'Study1', 'Female', 'CRC', '', '2021-12-23', 'Study 1', '1'],
            ['P1', 'D2', 'R4', 'Study2', 'Female', 'CRC', '', '2021-12-23', 'Study 2', '2'],
            ['P1', '', 'R3', 'Study1', '', '', '', '2021-12-24', 'Study 1', '1'],
            ['P1', '', 'R3', 'Study2', '', '', '', '2021-12-24', 'Study 2', '2'],
        ], columns=['Subject Id', 'Diagnosis Id', 'Radiology Id', 'Study Id',
                    'Gender', 'Diagnosis', 'Treatment', 'Radiology date', 'Study title', 'Individual study id'])
        expected_df.set_index(['Subject Id', 'Diagnosis Id', 'Radiology Id', 'Study Id'], inplace=True)
        pdt.assert_frame_equal(df, expected_df, check_dtype=False, check_categorical=False, check_like=True)


    def test_from_json_to_export_csr_df(self):
        csr_obs_path = os.path.join(os.path.dirname(__file__), 'csr_observations.json')
        input_json = json.loads(open(csr_obs_path).read())

        df = from_obs_json_to_export_csr_df(input_json)

        self.assertIsNotNone(df)
        actual_ids = list(df.index.values)
        self.assertEqual(actual_ids, [
            ('P1', 'D1', 'BS1', 'BM1', 'R1', 'STUDY1'),
            ('P1', 'D10', 'BS10', 'BM15', '', 'STUDY1'),
            ('P1', 'D10', 'BS10', 'BM9', '', 'STUDY1'),
            ('P2', 'D2', '', '', 'R2', 'STUDY1'),
            ('P2', '', '', '', 'R3', 'STUDY1'),
            ('P3', 'D11', 'BS2', 'BM2', '', 'STUDY1'),
            ('P3', 'D3', '', '', '', 'STUDY1'),
            ('P4', '', 'BS3', 'BM3', '', 'STUDY1'),
            ('P5', 'D12', 'BS11', 'BM10', '', 'STUDY1'),
            ('P5', 'D12', 'BS11', 'BM10', '', 'STUDY2'),
            ('P5', 'D5', 'BS12', 'BM11', '', 'STUDY1'),
            ('P5', 'D5', 'BS12', 'BM11', '', 'STUDY2'),
            ('P5', 'D5', 'BS4', 'BM12', '', 'STUDY1'),
            ('P5', 'D5', 'BS4', 'BM12', '', 'STUDY2'),
            ('P5', 'D5', 'BS4', 'BM4', '', 'STUDY1'),
            ('P5', 'D5', 'BS4', 'BM4', '', 'STUDY2'),
            ('P6', 'D6', 'BS5', 'BM13', '', 'STUDY2'),
            ('P6', 'D6', 'BS5', 'BM5', '', 'STUDY2'),
            ('P6', 'D6', 'BS9', 'BM8', '', 'STUDY2'),
            ('P7', 'D13', '', '', '', 'STUDY2'),
            ('P7', 'D7', 'BS6', '', '', 'STUDY2'),
            ('P8', 'D8', 'BS7', 'BM14', '', 'STUDY2'),
            ('P8', 'D8', 'BS7', 'BM6', '', 'STUDY2'),
            ('P9', 'D9', 'BS8', 'BM7', '', 'STUDY2')
        ])
        columns = list(df.columns.values)
        self.assertEqual(columns, [
            # patient
            '01. Date of birth',
            '01. Informed consent type',
            '02. Date informed Consent given',
            '02. Sex',
            '03. Date informed consent withdrawn',
            '04. Report hereditary susceptibility',
            'Taxonomy',
            # diagnosis
            '01. Date of diagnosis',
            '02. Tumor type',
            '03. Topography',
            '04. Tumor stage',
            '05. Center of diagnosis',
            'Treatment',
            # biosource
            '01. Biosource parent',
            '02. Date of biosource',
            '03. Tissue',
            '04. Disease status',
            '05. Tumor percentage',
            '06. Biosource dedicated for specific study',
            # biomaterial
            '01. Biomaterial parent',
            '02. Date of biomaterial',
            '03. Biomaterial type',
            # radiology
            '00. Radiology ID',
            '01. Examination Date',
            '02. Image Type',
            '03. Field Strength',
            '04. Individual ID',
            '05. Diagnosis ID',
            '06. Body Part',
            # study
            '01. Study ID',
            '02. Study acronym',
            '03. Study title',
            'Study datadictionary',
            '04. Individual Study ID'
        ])
        p5_study1_data = list(df.loc[
            ('P5', 'D12', 'BS11', 'BM10', '', 'STUDY1'),
            ['01. Study ID', '02. Study acronym', '03. Study title', '04. Individual Study ID']].values)
        self.assertEqual(p5_study1_data, ['STUDY1', 'STD1', 'Study 1', '5'])
        p5_study2_data = list(df.loc[
            ('P5', 'D12', 'BS11', 'BM10', '', 'STUDY2'),
            ['01. Study ID', '02. Study acronym', '03. Study title', '04. Individual Study ID']].values)
        self.assertEqual(p5_study2_data, ['STUDY2', 'STD2', 'Study 2', '1'])


if __name__ == '__main__':
    unittest.main()
