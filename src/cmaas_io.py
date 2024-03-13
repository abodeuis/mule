import os
import json
import rasterio
import multiprocessing
import numpy as np
import geopandas as gpd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from src.cmaas_types import CMAAS_Map, Legend, Layout, GeoReference, MapUnit, MapUnitType

### Legend
def loadLegendJson(filepath:Path, type_filter:MapUnitType=MapUnitType.ALL()) -> Legend:
    with open(filepath, 'r') as fh:
        json_data = json.load(fh)
    if json_data['version'] in ['5.0.1', '5.0.2']:
        legend = _loadUSGSLegendJson(filepath, type_filter)
    else:
        legend = _loadMULELegend(filepath, type_filter)
    return legend


def _loadUSGSLegendJson(filepath:Path, type_filter:MapUnitType=MapUnitType.ALL()) -> Legend:
    with open(filepath, 'r') as fh:
        json_data = json.load(fh)
    legend = Legend(provenance='USGS')
    for m in json_data['shapes']:
        # Filter out unwanted map unit types
        unit_type = MapUnitType.from_str(m['label'].split('_')[-1])
        if unit_type not in type_filter:
            continue
        # Remove type encoding from label
        unit_label = m['label']
        unit_label = ' '.join(unit_label.split('_'))
        if unit_type != MapUnitType.UNKNOWN:
            unit_label = ' '.join(unit_label.split(' ')[:-1])

        legend.features[unit_label] = MapUnit(label=unit_label, type=unit_type, bbox=np.array(m['points']).astype(int), provenance='USGS')
    return legend

def _loadMULELegend(filepath:Path, type_filter:MapUnitType=MapUnitType.ALL()) -> Legend:
    with open(filepath, 'r') as fh:
        json_data = json.load(fh)
    legend = Legend(provenance='MULE')
    # TODO - Implement MULE legend loading
    raise NotImplementedError('MULE legend loading not yet implemented')
    return legend

def saveMULELegend(filepath:Path, legend:Legend):
    raise NotImplementedError('MULE legend saving not yet implemented')

def parallelLoadLegends(filepaths, type_filter:MapUnitType=MapUnitType.ALL(), threads:int=32):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        legends = {}
        for filepath in filepaths:
            map_name = os.path.basename(os.path.splitext(filepath)[0])
            legends[map_name] = executor.submit(loadLegendJson, filepath, type_filter).result()
    return legends

### Layout
def loadLayoutJson(filepath:Path) -> Layout:
    with open(filepath, 'r') as fh:
        layout_version = 1
        try:
            for line in fh:
                json_data = json.loads(line)
                if json_data['name'] == 'segmentation':
                    layout_version = 2
                break
        except:
            layout_version = 1
            pass
        if layout_version == 1:
            layout = _loadUnchartedLayoutv1Json(filepath)
        else:
            layout = _loadUnchartedLayoutv2Json(filepath)
    return layout

def _loadUnchartedLayoutv1Json(filepath:Path) -> Layout:
    layout = Layout()
    layout.provenance = 'Uncharted'
    with open(filepath, 'r') as fh:
        json_data = json.load(fh)

    for section in json_data:
        bounds = np.array(section['bounds']).astype(int)
        if section['name'] == 'legend_points_lines': # Intentionally not using elif so can be overwritten
            layout.point_legend = bounds
            layout.line_legend = bounds
        if section['name'] == 'map':
            layout.map = bounds
        elif section['name'] == 'correlation_diagram':
            layout.correlation_diagram = bounds
        elif section['name'] == 'cross_section':
            layout.cross_section = bounds
        elif section['name'] == 'legend_points':
            layout.point_legend = bounds
        elif section['name'] == 'legend_lines':
            layout.line_legend = bounds
        elif section['name'] == 'legend_polygons':
            layout.polygon_legend = bounds
    return layout

def _loadUnchartedLayoutv2Json(filepath:Path) -> Layout:
    layout = Layout()
    layout.provenance = 'Uncharted'
    with open(filepath, 'r') as fh:
        for line in fh:
            json_data = json.loads(line)
            section_name = json_data['model']['field']
            bounds = np.array(json_data['bounds']).astype(int)
            if section_name == 'legend_points_lines': # Intentionally not using elif so can be overwritten
                layout.point_legend = bounds
                layout.line_legend = bounds
            if section_name == 'map':
                layout.map = bounds
            elif section_name == 'correlation_diagram':
                layout.correlation_diagram = bounds
            elif section_name == 'cross_section':
                layout.cross_section = bounds
            elif section_name == 'legend_points':
                layout.point_legend = bounds
            elif section_name == 'legend_lines':
                layout.line_legend = bounds
            elif section_name == 'legend_polygons':
                layout.polygon_legend = bounds
    return layout

def saveMULELayout(filepath:Path, layout:Layout):
    # TODO
    raise NotImplementedError('MULE layout saving not yet implemented')

def parallelLoadLayouts(filepaths, threads:int=32):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        layouts = {}
        for filepath in filepaths:
            map_name = os.path.basename(os.path.splitext(filepath)[0])
            layouts[map_name] = executor.submit(loadLayoutJson, filepath).result()
    return layouts

### GeoReference
# TODO
def loadGeoReference(filepath:Path) -> GeoReference:
    # TODO
    raise NotImplementedError('GeoReference loading not yet implemented')

def saveMULEGeoReference(filepath:Path, georef:GeoReference):
    # TODO
    raise NotImplementedError('MULE georef saving not yet implemented')

### Map Metadata
# TODO
def loadMapMetadata(filepath:Path):
    # TODO
    raise NotImplementedError('Map metadata loading not yet implemented')

def saveMULEMapMetadata(filepath:Path, metadata):
    # TODO
    raise NotImplementedError('MULE metadata saving not yet implemented')

### GeoTiff
def loadGeoTiff(filepath:Path):
    """Load a GeoTiff file. Image is in CHW format. Raises exception if image is not loaded properly. Returns a tuple of the image, crs and transform """
    with rasterio.open(filepath) as fh:
        image = fh.read()
        crs = fh.crs
        transform = fh.transform
    if image is None:
        msg = f'Unknown issue caused "{filepath}" to fail while loading'
        raise Exception(msg)
    
    return image, crs, transform

def saveGeoTiff(filename, image, crs, transform):
    image = np.array(image[...], ndmin=3)
    with rasterio.open(filename, 'w', driver='GTiff', compress='lzw', height=image.shape[1], width=image.shape[2],
                       count=image.shape[0], dtype=image.dtype, crs=crs, transform=transform) as fh:
        fh.write(image)

def parallelLoadGeoTiffs(filepaths, processes=multiprocessing.cpu_count()): # -> list[tuple(image, crs, transfrom)]:
    """Load a list of filenames in parallel with N processes. Returns a list of images"""
    p=multiprocessing.Pool(processes=processes)
    with multiprocessing.Pool(processes) as p:
        images = p.map(loadGeoTiff, filepaths)

    return images

### Map
def loadCMASSMap(image_path:Path, legend_path:Path=None, layout_path:Path=None, georef_path:Path=None, metadata_path:Path=None) -> CMAAS_Map:
    map_name = os.path.basename(os.path.splitext(image_path)[0])

    # Start Threads
    with ThreadPoolExecutor() as executor:
        img_future = executor.submit(loadGeoTiff, image_path)
        if legend_path is not None:
            lgd_future = executor.submit(loadLegendJson, legend_path)
        if layout_path is not None:
            lay_future = executor.submit(loadLayoutJson, layout_path)
        ### Not implemented yet
        # if georef_path is not None:
        #     gr_future = executor.submit(loadGeoReference, georef_path)
        # if metadata_path is not None:
        #     md_future = executor.submit(loadMapMetadata, metadata_path)
        
        image, crs, transform = img_future.result()
        if legend_path is not None:
            legend = lgd_future.result()
        if layout_path is not None:
            layout = lay_future.result()
        ### Not implemented yet
        # if georef_path is not None:
        #     georef = gr_future.result()
        # else:
        georef = GeoReference(crs=crs, transform=transform, provenance='GeoTIFF')
        # if metadata_path is not None:
        #     metadata = md_future.result()
    
    map_data = CMAAS_Map(map_name, image, georef=georef)
    if legend_path is not None:
        map_data.legend = legend
    if layout_path is not None:
        map_data.layout = layout
    ### Not implemented yet
    # if metadata_path is not None:
    #     map_data.metadata = metadata

    return map_data

def loadCMASSMapMULE(image_path:Path, mule_path:Path):
    # TODO
    pass

def saveCMASSMapMULE(filepath, map_data:CMAAS_Map):
    # TODO
    pass

def parallelLoadCMASSMaps(map_files, legend_path=None, layout_path=None, processes : int=multiprocessing.cpu_count()):
    """Load a list of maps in parallel with N processes. Returns a list of CMASS_Map objects"""
    # Build argument list
    map_args = []
    for file in map_files:
        map_name = os.path.basename(os.path.splitext(file)[0])
        lgd_file = None
        if legend_path is not None:
            lgd_file = os.path.join(legend_path, f'{map_name}.json')
            if not os.path.exists(lgd_file):
                lgd_file = None
        lay_file = None
        if layout_path is not None:
            lay_file = os.path.join(layout_path, f'{map_name}.json')
            if not os.path.exists(lay_file):
                lay_file = None
        map_args.append((file, lgd_file, lay_file))

    # Load all files in parallel
    with multiprocessing.Pool(processes) as p:
        results = p.starmap(loadCMASSMap, map_args)

    return results


### Segmentations
def saveGeoJson(filepath:Path, geoDataFrame:gpd.GeoDataFrame):
    if os.path.splitext(filepath)[-1] not in ['.json','.geojson']:
        filename += '.geojson'
    geoDataFrame.to_crs('EPSG:4326')
    geoDataFrame.to_file(filepath, driver='GeoJSON')

def saveGeopackage(filepath, map_data:CMAAS_Map):
    # TODO
    pass