import src.cmaas_io as io
import src.utils as utils
from src.vertical_processing_pipeline import vertical_processing_pipeline


# Step 0
def load_map_wrapper(image_path, legend_path=None, layout_path=None, georef_path=None):
    """Wrapper with a custom display for the monitor"""
    map_data = io.loadCMASSMap(image_path, legend_path, layout_path, georef_path)
    vertical_processing_pipeline.log_to_monitor(map_data.name, {'Shape': map_data.shape})
    return map_data

# Step 1
def add_precomputed_layout_data(map_data, layout_file):
    def get_map_region_size(map_data):
        _, height, width = map_data.image.shape
        if map_data.layout is not None:
            if map_data.layout.map is not None:
                min_xy, max_xy = utils.boundingBox(map_data.layout.map)
                height = max_xy[1] - min_xy[1]
                width = max_xy[0] - min_xy[0]
        return height, width
    
    map_data.layout = io.loadLayoutJson(layout_file)
    height, width = get_map_region_size(map_data)
    vertical_processing_pipeline.log_to_monitor(map_data.name, {'Map Region': f'{height}, {width}'})
    return map_data

def add_precomputed_legend_data(map_data, legend_file):
    map_data.legend = io.loadLegendJson(legend_file)
    vertical_processing_pipeline.log_to_monitor(map_data.name, {'Map Units': f'{len(map_data.legend.features)}'})
    return map_data

def load_pattern_model(filepath):
    from src.pattern_model.pattern_classification import MapUnitClassifier
    classifier_model = MapUnitClassifier(model_path=filepath,image_size=224)
    return classifier_model

def classify_legend_pattern(map_data, classifier_model, image_size = 224):
    map_data = classifier_model.inference_map_data(map_data)
    return map_data