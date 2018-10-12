import unittest
from packer.table_transformations.patient_diagnosis_biosource_biomaterial import \
    to_patient_diagnosis_biosource_biomaterial_dataframe
import pandas as pd
import pandas.testing as pdt


class PatientDiagnosisBiosourceBiomaterialTranformations(unittest.TestCase):

    def test_no_observations(self):
        observations_api_response = {
            'dimensionDeclarations': [],
            'cells': [],
            'dimensionElements': {}
        }

        df = to_patient_diagnosis_biosource_biomaterial_dataframe(observations_api_response)

        self.assertIsNotNone(df)
        self.assertEqual(df.size, 0)

    def test_result_data_shape(self):
        observations_api_response = {
            "dimensionDeclarations": [
                {
                    "name": "study",
                },
                {
                    "name": "concept",
                },
                {
                    "name": "patient",
                },
                {
                    "name": "PMC Diagnosis ID",
                },
                {
                    "name": "PMC Biosource ID",
                },
                {
                    "name": "PMC Biomaterial ID",
                },
            ],
            "cells": [
                {
                    "dimensionIndexes": [0, 0, 0, None, None, None], "numericValue": 42,
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 1, 0, 0, None, None], "stringValue": "Diagnosis 1 Name",
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 2, 0, 0, 0, None], "stringValue": "Skin",
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 3, 0, 0, 0, 0], "stringValue": "Tue Apr 24 02:00:00 CEST 2018",
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 3, 0, 0, 0, 1], "stringValue": "Wed Mar 07 01:00:00 CET 2018",
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 0, 1, None, None, None], "numericValue": 39,
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 1, 1, 0, None, None], "stringValue": "Diagnosis 1 Name 2",
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 2, 1, 0, 1, None], "stringValue": "Liver",
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 3, 1, 0, 1, 2], "stringValue": "Fri Jan 19 01:00:00 CET 2018",
                    "inlineDimensions": [],
                },
                {
                    "dimensionIndexes": [0, 3, 1, 0, 1, 3], "stringValue": "Sun Jun 05 02:00:00 CEST 2011",
                    "inlineDimensions": [],
                },
            ],
            "dimensionElements": {
                "study": [
                    {
                        "name": "TEST"
                    }
                ],
                "concept": [
                    {
                        "conceptPath": "\\Patient\\Age\\",
                        "conceptCode": "patient_concept_1",
                        "name": "Age"
                    },
                    {
                        "conceptPath": "\\Patient\\Diagnosis\\Diagnosis Name\\",
                        "conceptCode": "diagnosis_concept_1",
                        "name": "Diagnosis Name"
                    },
                    {
                        "conceptPath": "\\Patient\\Diagnosis\\Biosource\\Cell type\\",
                        "conceptCode": "biosource_concept_1",
                        "name": "Cell type"
                    },
                    {
                        "conceptPath": "\\Patient\\Diagnosis\\Biosource\\Biomaterial\\Date\\",
                        "conceptCode": "biomaterial_concept_1",
                        "name": "Date of biomaterial"
                    },
                ],
                "patient": [
                    {
                        "id": 1,
                        "trial": "P1",
                    },
                    {
                        "id": 2,
                        "trial": "P2",
                    }
                ],
                "PMC Diagnosis ID": [
                    "D1"
                ],
                "PMC Biosource ID": [
                    "BS1",
                    "BS2"
                ],
                "PMC Biomaterial ID": [
                    "BM1",
                    "BM2",
                    "BM3",
                    "BM4"
                ],
            }
        }

        df = to_patient_diagnosis_biosource_biomaterial_dataframe(observations_api_response)

        self.assertIsNotNone(df)
        pdt.assert_frame_equal(df, pd.DataFrame([
            ['P1', 'D1', 'BS1', 'BM1', 42.0, 'Tue Apr 24 02:00:00 CEST 2018', 'Skin', 'Diagnosis 1 Name'],
            ['P1', 'D1', 'BS1', 'BM2', 42.0, 'Wed Mar 07 01:00:00 CET 2018', 'Skin', 'Diagnosis 1 Name'],
            ['P2', 'D1', 'BS2', 'BM3', 39.0, 'Fri Jan 19 01:00:00 CET 2018', 'Liver', 'Diagnosis 1 Name 2'],
            ['P2', 'D1', 'BS2', 'BM4', 39.0, 'Sun Jun 05 02:00:00 CEST 2011', 'Liver', 'Diagnosis 1 Name 2'],
        ], columns=['Patient ID', 'PMC Diagnosis ID', 'PMC Biosource ID',
                                                             'PMC Biomaterial ID', 'Age', 'Date of biomaterial',
                                                             'Cell type', 'Diagnosis Name']))


if __name__ == '__main__':
    unittest.main()
