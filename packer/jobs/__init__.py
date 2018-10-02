from .tm_conversion_export import tm_conversion_export
from .example import add
from .export_maxima import export_maxima

registry = {
    'add': add,
    'export_maxima': export_maxima,
    'tm_conversion_export': tm_conversion_export
}
