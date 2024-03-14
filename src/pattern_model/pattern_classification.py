import tensorflow as tf
import numpy as np
from tensorflow import keras  
from keras.models import load_model
import tensorflow_hub as hub

import src.utils as utils

class MapUnitClassifier:
    def __init__(self, image_size, model_path='/projects/bbym/nathanj/attentionUnet/pattern/pattern_clasification_model.hdf5'):
        # Load configuration
        self.model_path = model_path
        self.image_size = (image_size, image_size)

        # Load the model within the initialization 
        with keras.utils.custom_object_scope({'KerasLayer': hub.KerasLayer}):
            self.model = load_model(self.model_path) 

    def inference_map_data(self, map_data):
        image = map_data.image

        # Get the size of the map
        map_channels, map_height, map_width = image.shape

        # Reshape maps with 1 channel images (greyscale) to 3 channels for inference
        if map_channels == 1: # This is tmp fix!    
            image = np.concatenate([image,image,image], axis=0)

        # Prep data
        legend_images = []
        legend_labels = []
        for label, map_unit in map_data.legend.features.items():
            legend_labels.append(label)
            min_pt, max_pt = utils.boundingBox(map_unit.contour) # Need this as points order can be reverse or could have quad
            legend_img = map_data.image[:,min_pt[1]:max_pt[1], min_pt[0]:max_pt[0]]
            legend_img = tf.image.resize(legend_img, (self.image_size, self.image_size))
            legend_img = legend_img / 255.0  # Normalize pixel values
            legend_images.concatenate(legend_img, axis=0)
        
        # Run model to get pattern classification
        predictions = self.model.predict(legend_images.transpose(0,2,3,1))

        # Interpret predictions (convert to binary class labels)
        results = np.squeeze((predictions > 0.5)).astype(int) 

        # Set pattern results in map data
        for label, cls in zip(legend_labels, results):
            map_data.legend.features[label].pattern = bool(cls)

        return map_data
        