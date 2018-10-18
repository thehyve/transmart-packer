from .patient_diagnosis_biosource_biomaterial_export import patient_diagnosis_biosource_biomaterial_export
from .example import add
from .basic_export import basic_export

registry = {
    'add': add,
    'basic_export': basic_export,
    'patient_diagnosis_biosource_biomaterial_export': patient_diagnosis_biosource_biomaterial_export
}
