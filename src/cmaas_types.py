import rasterio
from rasterio.crs import CRS
from rasterio.control import GroundControlPoint
from rasterio.transform import from_gcps
from enum import Enum
from typing import List, Tuple
import numpy as np

DEBUG_MODE = True # Turns on debuging why two objects are not equal

class MapUnitType(Enum):
    POINT = 1
    LINE = 2
    POLYGON = 3
    UNKNOWN = 4
    def ALL():
        return [MapUnitType.POINT, MapUnitType.LINE, MapUnitType.POLYGON, MapUnitType.UNKNOWN]
    def ALL_KNOWN():
        return [MapUnitType.POINT, MapUnitType.LINE, MapUnitType.POLYGON]

    def from_str(feature_type:str):
        if feature_type.lower() in ['pt','point']:
            return MapUnitType.POINT
        elif feature_type.lower() in ['line']:
            return MapUnitType.LINE
        elif feature_type.lower() in ['poly','polygon']:
            return MapUnitType.POLYGON
        else:
            return MapUnitType.UNKNOWN
        
    def to_str(self):
        if self == MapUnitType.POINT:
            return 'pt'
        elif self == MapUnitType.LINE:
            return 'line'
        elif self == MapUnitType.POLYGON:
            return 'poly'
        else:
            return 'unknown'

    def __str__(self) -> str:
        return self.to_str()
    
    def __repr__(self) -> str:
        repr_str = 'MapUnitType.'
        if self == MapUnitType.POINT:
            repr_str += 'POINT'
        elif self == MapUnitType.LINE:
            repr_str += 'LINE'
        elif self == MapUnitType.POLYGON:
            repr_str += 'POLYGON'
        else:
            repr_str += 'Unknown'
        return repr_str

class MapUnit():
    def __init__(self, type=MapUnitType.UNKNOWN, label=None, abbreviation=None, description=None, color=None, pattern=None, overlay=False, bbox=None, provenance=None):
        # MapUnit Legend Information
        self.type = type
        self.label = label
        self.abbreviation = abbreviation
        self.description = description
        self.color = color
        self.pattern = pattern
        self.overlay = overlay
        self.bbox = bbox
        self.provenance = provenance

        # MapUnit Map Segmentation
        self.mask = None
        self.mask_confidence = None
        self.geometry = None
    
    def to_dict(self):
        # We don't save the map segmentation data in the dictionary
        return {
            'type' : self.type.to_str() if self.type is not None else 'unknown',
            'label' : self.label,
            'abbreviation' : self.abbreviation,
            'description' : self.description,
            'color' : self.color,
            'pattern' : self.pattern,
            'overlay' : self.overlay,
            'bounding_box' : self.bbox.to_list() if self.bbox is not None else None
        }
    
    def __eq__(self, __value: object) -> bool:
        if self.type != __value.type:
            if DEBUG_MODE:
                print(f'Type Mismatch: {self.type} != {__value.type}')
            return False
        if self.label != __value.label:
            if DEBUG_MODE:
                print(f'Label Mismatch: {self.label} != {__value.label}')
            return False
        if self.abbreviation != __value.abbreviation:
            if DEBUG_MODE:
                print(f'Abbreviation Mismatch: {self.abbreviation} != {__value.abbreviation}')
            return False
        if self.description != __value.description:
            if DEBUG_MODE:
                print(f'Description Mismatch: {self.description} != {__value.description}')
            return False
        if self.color != __value.color:
            if DEBUG_MODE:
                print(f'Color Mismatch: {self.color} != {__value.color}')
            return False
        if self.pattern != __value.pattern:
            if DEBUG_MODE:
                print(f'Pattern Mismatch: {self.pattern} != {__value.pattern}')
            return False
        if self.overlay != __value.overlay:
            if DEBUG_MODE:
                print(f'Overlay Mismatch: {self.overlay} != {__value.overlay}')
            return False
        if isinstance(self.bbox, (np.ndarray, np.generic)) or isinstance(__value.bbox, (np.ndarray, np.generic)):
            if (self.bbox != __value.bbox).any():
                if DEBUG_MODE:
                    print(f'Bounding Box Mismatch: {self.bbox} != {__value.bbox}')
                return False
        else:
            if self.bbox != __value.bbox:
                if DEBUG_MODE:
                    print(f'Bounding Box Mismatch: {self.bbox} != {__value.bbox}')
                return False
        if self.provenance != __value.provenance:
            if DEBUG_MODE:
                print(f'Provenance Mismatch: {self.provenance} != {__value.provenance}')
            return False
        return True

    def __str__(self) -> str:
        out_str = 'CMASS_Feature{\'' + self.label + '\'}'
        return out_str

    def __repr__(self) -> str:
        repr_str = 'CMASS_Feature{'
        repr_str += f'type : \'{self.type}\', '
        repr_str += f'label : \'{self.label}\', '
        repr_str += f'abbreviation : \'{self.abbreviation}\', '
        repr_str += f'description : \'{self.description}\', '
        repr_str += f'color : \'{self.color}\', '
        repr_str += f'pattern : \'{self.pattern}\', '
        #repr_str += f'mask : {self.mask.shape}, ' if self.mask is not None else f'mask : {self.mask}, ',
        repr_str += f'mask_confidence : {self.mask_confidence}'
        return repr_str

class Legend():
    def __init__(self, features=None, provenance=None):
        self.features = features if features is not None else {}
        self.provenance = provenance

    def to_dict(self):
        feature_dict = {}
        for label, map_unit in self.features.items():
            feature_dict[label] = map_unit.to_dict()
        return {
            'features' : feature_dict,
            'provenance' : self.provenance
        }
    
    def map_unit_distr(self):
        dist = {}
        for feature in self.features:
            if feature.type in dist:
                dist[feature.type].append(feature.label)
            else:
                dist[feature.type] = [feature.label]
        return dist

    def __len__(self):
        return len(self.features)
    
    def __eq__(self, __value: object) -> bool:
        if self.provenance != __value.provenance:
            if DEBUG_MODE:
                print(f'Provenance Mismatch: {self.provenance} != {__value.provenance}')
            return False
        for unit in self.features:
            if unit not in __value.features:
                if DEBUG_MODE:
                    print(f'Feature Mismatch: {unit} not in {__value.features}')
                return False
            if self.features[unit] != __value.features[unit]:
                if DEBUG_MODE:
                    print(f'Feature Mismatch: {self.features[unit]} != {__value.features[unit]}')
                return False
        return True

    def __str__(self) -> str:
        out_str = 'CMASS_Legend{' + f'{len(self.features)} Features : {self.features.keys()}' + '}'
        return out_str
    
    def __repr__(self) -> str:
        repr_str = 'CMASS_Legend{Provenance : ' + f'{self.provenance}, {len(self.features)} Features : {self.features}' + '}'
        return repr_str

class Layout():
    def __init__(self, map=None, legend=None, correlation_diagram=None, cross_section=None, point_legend=None, line_legend=None, polygon_legend=None, provenance=None):
        self.map = map
        self.correlation_diagram = correlation_diagram
        self.cross_section = cross_section
        self.point_legend = point_legend
        self.line_legend = line_legend
        self.polygon_legend = polygon_legend
        self.provenance = provenance

    def __str__(self) -> str:
        out_str = 'CMASS_Layout{'
        out_str += f'map : {self.map}, '
        out_str += f'correlation_diagram : {self.correlation_diagram}, '
        out_str += f'cross_section : {self.cross_section}, '
        out_str += f'point_legend : {self.point_legend}, '
        out_str += f'line_legend : {self.line_legend}, '
        out_str += f'polygon_legend : {self.polygon_legend}, '
        out_str += f'provenance : {self.provenance}'
        out_str += '}'
        return out_str
    
    def __repr__(self) -> str:
        out_str = 'CMASS_Layout{'
        out_str += f'map : {self.map}, '
        out_str += f'correlation_diagram : {self.correlation_diagram}, '
        out_str += f'cross_section : {self.cross_section}, '
        out_str += f'point_legend : {self.point_legend}, '
        out_str += f'line_legend : {self.line_legend}, '
        out_str += f'polygon_legend : {self.polygon_legend}, '
        out_str += f'provenance : {self.provenance}'
        out_str += '}'
        return out_str

    def __eq__(self, __value: object) -> bool:
        if isinstance(self.map, (np.ndarray, np.generic)) and isinstance(__value.map, (np.ndarray, np.generic)):
            if (self.map != __value.map).any():
                if DEBUG_MODE:
                    print(f'Map Mismatch: {self.map} != {__value.map}')
                return False
        else:
            if self.map != __value.map:
                if DEBUG_MODE:
                    print(f'Map Mismatch: {self.map} != {__value.map}')
                return False
        if isinstance(self.correlation_diagram, (np.ndarray, np.generic)) or isinstance(__value.correlation_diagram, (np.ndarray, np.generic)):
            if (self.correlation_diagram != __value.correlation_diagram).any():
                if DEBUG_MODE:
                    print(f'Correlation Diagram Mismatch: {self.correlation_diagram} != {__value.correlation_diagram}')
                return False
        else:
            if self.correlation_diagram != __value.correlation_diagram:
                if DEBUG_MODE:
                    print(f'Correlation Diagram Mismatch: {self.correlation_diagram} != {__value.correlation_diagram}')
                return False
        if isinstance(self.cross_section, (np.ndarray, np.generic)) or isinstance(__value.cross_section, (np.ndarray, np.generic)):
            if (self.cross_section != __value.cross_section).any():
                if DEBUG_MODE:
                    print(f'Cross Section Mismatch: {self.cross_section} != {__value.cross_section}')
                return False
        else:
            if self.cross_section != __value.cross_section:
                if DEBUG_MODE:
                    print(f'Cross Section Mismatch: {self.cross_section} != {__value.cross_section}')
                return False
        if isinstance(self.point_legend, (np.ndarray, np.generic)) or isinstance(__value.point_legend, (np.ndarray, np.generic)):
            if (self.point_legend != __value.point_legend).any():
                if DEBUG_MODE:
                    print(f'Point Legend Mismatch: {self.point_legend} != {__value.point_legend}')
                return False
        else:
            if self.point_legend != __value.point_legend:
                if DEBUG_MODE:
                    print(f'Point Legend Mismatch: {self.point_legend} != {__value.point_legend}')
                return False
        if isinstance(self.line_legend, (np.ndarray, np.generic)) or isinstance(__value.line_legend, (np.ndarray, np.generic)):
            if (self.line_legend != __value.line_legend).any():
                if DEBUG_MODE:
                    print(f'Line Legend Mismatch: {self.line_legend} != {__value.line_legend}')
                return False
        else:
            if self.line_legend != __value.line_legend:
                if DEBUG_MODE:
                    print(f'Line Legend Mismatch: {self.line_legend} != {__value.line_legend}')
                return False
        if isinstance(self.polygon_legend, (np.ndarray, np.generic)) or isinstance(__value.polygon_legend, (np.ndarray, np.generic)):
            if (self.polygon_legend != __value.polygon_legend).any():
                if DEBUG_MODE:
                    print(f'Polygon Legend Mismatch: {self.polygon_legend} != {__value.polygon_legend}')
                return False
        else:
            if self.polygon_legend != __value.polygon_legend:
                if DEBUG_MODE:
                    print(f'Polygon Legend Mismatch: {self.polygon_legend} != {__value.polygon_legend}')
                return False
        if self.provenance != __value.provenance:
            if DEBUG_MODE:
                print(f'Provenance Mismatch: {self.provenance} != {__value.provenance}')
            return False
        return True

class GeoReference():
    def __init__(self, crs:CRS=None, transform:rasterio.transform.Affine=None, gcps:List[GroundControlPoint]=None, confidence:float=None, provenance=None):
        self.crs = crs
        self.transform = transform
        self.gcps = gcps
        self.confidence = confidence
        self.provenance = provenance

    def __eq__(self, __value: object) -> bool:
        # Mark an object as equal if its crs and transform are equal
        if self.crs is not None and self.transform is not None and __value.crs is not None and __value.transform is not None:
            if self.crs != __value.crs:
                if DEBUG_MODE:
                    print(f'CRS Mismatch: {self.crs} != {__value.crs}')
                return False
            if self.transform != __value.transform:
                if DEBUG_MODE:
                    print(f'Transform Mismatch: {self.transform} != {__value.transform}')
                return False
        # Otherwise test on the gcps and provenance
        else:
            if self.gcps != __value.gcps:
                if DEBUG_MODE:
                    print(f'GCP Mismatch: {self.gcps} != {__value.gcps}')
                return False
            if self.provenance != __value.provenance:
                if DEBUG_MODE:
                    print(f'Provenance Mismatch: {self.provenance} != {__value.provenance}')
                return False
        return True

class CMAAS_MapMetadata():
    def __init__(self, provenance:str, title:str, authors:List[str], publisher:str, url:str, source_url:str, year:int, organization:str, color_type:str, physiographic_region:str, scale:str, shape_type:str):
        self.title = title
        self.authors = authors
        self.publisher = publisher # Is this signifgantly difference then organization?
        self.url = url # What is the diff between url and source url.
        self.source_url = source_url
        self.provenance = provenance

        # Gold standard Validation criteria
        self.year = year
        self.organization = organization  # Source
        self.color_type = color_type # E.g. full color, monochrome
        self.physiographic_region = physiographic_region # I need a resource that can display the possible values for this
        self.scale = scale # E.g. 1:24,000 
        self.shape_type = shape_type # Square vs non-square

class CMAAS_Map():
    def __init__(self, name:str, image, georef:GeoReference=None, legend:Legend=None, layout:Layout=None, metadata:CMAAS_MapMetadata=None):
        self.name = name
        self.image = image
        self.georef = georef
        self.legend = legend
        self.layout = layout
        self.metadata = metadata
        # Segmentation mask
        self.mask = None

        # Utility field
        self.shape = self.image.shape
    
    def __eq__(self, __value: object) -> bool:
        if self.name != __value.name:
            if DEBUG_MODE:
                print(f'Name Mismatch: {self.name} != {__value.name}')
            return False
        if self.image.shape != __value.image.shape:
            if DEBUG_MODE:
                print(f'Shape Mismatch: {self.image.shape} != {__value.image.shape}')
            return False
        if self.georef != __value.georef:
            if DEBUG_MODE:
                print(f'GeoReference Mismatch: {self.georef} != {__value.georef}')
            return False
        if self.legend != __value.legend:
            if DEBUG_MODE:
                print(f'Legend Mismatch: {self.legend} != {__value.legend}')
            return False
        if self.layout != __value.layout:
            if DEBUG_MODE:
                print(f'Layout Mismatch: {self.layout} != {__value.layout}')
            return False
        # Not implemented yet
        # if self.metadata != __value.metadata:
        #     if DEBUG_MODE:
        #         print(f'Metadata Mismatch: {self.metadata} != {__value.metadata}')
        #     return False
        return True

    def __str__(self) -> str:
        out_str = 'CMASS_Map{'
        out_str += f'name : \'{self.name}\', '
        out_str += f'image : {self.shape}, '
        out_str += f'georef : {self.georef}, '
        out_str += f'legend : {self.legend}, '
        out_str += f'layout : {self.layout}, '
        out_str += f'metadata : {self.metadata}'
        out_str += '}'
        return out_str

    def __repr__(self) -> str:
        repr_str = 'CMASS_Map{'
        repr_str += f'name : \'{self.name}\', '
        repr_str += f'image : {self.shape}, '
        repr_str += f'georef : {self.georef}, '
        repr_str += f'legend : {self.legend}, '
        repr_str += f'layout : {self.layout}, '
        repr_str += f'metadata : {self.metadata}'
        repr_str += '}'
        return repr_str
    

