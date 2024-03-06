import os
import copy
import json
import numpy as np
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import src.cmaas_io as io
import src.utils as utils
from src.cmaas_types import Legend, MapUnit, MapUnitType

def loadStepUpLegendJson(filepath:Path, type_filter:MapUnitType=MapUnitType.ALL) -> Legend:
    with open(filepath, 'r') as f:
        json_data = json.load(f)
    legend = Legend(provenance='USGS')
    for m in json_data['shapes']:
        # Filter out unwanted map unit types
        unit_type = MapUnitType.from_str(m['label'].split(' ')[0])
        if unit_type not in type_filter:
            continue
        # Remove type encoding from label
        unit_label = m['label']
        if unit_type != MapUnitType.UNKNOWN:
            unit_label = '_'.join(unit_label.split('_')[:-1])

        # Check for description
        if unit_label in legend.features:
            box1min, box1max = utils.boundingBox(legend.features[unit_label].bbox)
            box2min, box2max = utils.boundingBox(np.array(m['points']).astype(int))
            if ((box1max[0] - box1min[0]) * (box1max[1] - box1min[1])) > ((box2max[0] - box2min[0]) * (box2max[1] - box2min[1])):
                legend.features[unit_label].description = legend.features[unit_label].bbox
                legend.features[unit_label].bbox = np.array(m['points']).astype(int)
            else:
                legend.features[unit_label].description = np.array(m['points']).astype(int)
        else:
            legend.features[unit_label] = MapUnit(label=unit_label, type=unit_type, bbox=np.array(m['points']).astype(int), provenance='USGS')
    return legend

def parallelLoadStepUpLegends(filepaths, type_filter:MapUnitType=MapUnitType.ALL(), threads:int=32):
    with ThreadPoolExecutor(max_workers=threads) as executor:
        legends = {}
        for filepath in filepaths:
            map_name = os.path.basename(os.path.splitext(filepath)[0])
            legends[map_name] = executor.submit(loadStepUpLegendJson, filepath, type_filter).result()
    return legends

def saveSteupUpJson(filepath:Path, legend:Legend):
    output_json = {}
    output_json['version'] = '5.0.1'
    output_json['flags'] = {'source' : "Post_Fixed"}
    output_json['shapes'] = []
    for label, map_unit in legend.features.items():
        map_unit.label = map_unit.label.replace(' ', '_')
        if map_unit.type == MapUnitType.UNKNOWN:
            unit_label = map_unit.label
        else:
            unit_label = map_unit.label + '_' + map_unit.type.to_str()
        output_json['shapes'].append({'label' : unit_label, 'points' : map_unit.bbox.tolist(), 'group_id' : None, 'shape_type' : 'rectangle', 'flags' : {}})
        if map_unit.description is not None:
            unit_label = unit_label + '_desc'
            output_json['shapes'].append({'label' : unit_label, 'points' : map_unit.description.tolist(), 'group_id' : None, 'shape_type' : 'rectangle', 'flags' : {}})

    with open(filepath, 'w') as f:
        json.dump(output_json, f)

def addFeature(feature, legend):
    if feature.description is not None:
        feature_desc = copy.deepcopy(feature)
        feature_desc.label = feature.label + '_desc'
        feature_desc.bbox = feature.description
        feature_desc.description = None
        feature.description = None
        legend.features[feature.label] = feature
        legend.features[feature_desc.label] = feature_desc
    else:
        legend.features[feature.label] = feature
    return legend

def main():
    output_dir = 'postfixed_legends'
    truth_dir = ['../../datasets/training/usgs_legends', '../../datasets/validation/usgs_legends', '../../datasets/final_evaluation/usgs_legends']
    test_dir = '../../datasets/step_up_jsons/legends'

    # Load Data
    data_files = []
    for dir in truth_dir:
        data_files.append([os.path.join(dir, f) for f in os.listdir(dir) if f.endswith('.json')])
    data_files = [file for sublist in data_files if sublist is not None for file in sublist]

    test_files = [os.path.join(test_dir, f) for f in os.listdir(test_dir) if f.endswith('.json')]

    print('Loading True legends')
    true_legends = io.parallelLoadLegends(data_files)

    print('Loading Test legends')
    test_legends = parallelLoadStepUpLegends(test_files)
    for l in test_legends.values():
        for f in l.features.values():
            print(f.__repr__())
        break

    # Make output directory
    os.makedirs(output_dir, exist_ok=True)

    print('generating truth dictionary')
    true_legend_dict = {'pts' : [], 'lines' : [], 'polygons' : [], 'unknown' : []}
    for legend in tqdm(true_legends.values()):
        for feature in legend.features.values():
            if feature.type == MapUnitType.POINT:
                true_legend_dict['pts'].append(feature.label.lower())
            elif feature.type == MapUnitType.LINE:
                true_legend_dict['lines'].append(feature.label.lower())
            elif feature.type == MapUnitType.POLYGON:
                true_legend_dict['polygons'].append(feature.label.lower())
            else:
                true_legend_dict['unknown'].append(feature.label.lower())

    print(f'True dictionary contains {len(true_legend_dict["pts"])} points, {len(true_legend_dict["lines"])} lines, {len(true_legend_dict["polygons"])} polygons, and {len(true_legend_dict["unknown"])} unknowns')
    print(f'True dictionary contains {len(set(true_legend_dict["pts"]))} unique points, {len(set(true_legend_dict["lines"]))} unique lines, {len(set(true_legend_dict["polygons"]))} unique polygons, and {len(set(true_legend_dict["unknown"]))} unique unknowns')

    known_lines = ['monocline','syncline','anticline','antiform','synform']
    for line in known_lines:
        true_legend_dict['lines'].append(line)

    output_dict = {}
    stat_dict = {'pts' : [], 'lines' : [], 'likely_lines' : [], 'polygons' : [], 'likely_poly' : [], 'unknown' : []}
    for map, legend in tqdm(test_legends.items()):
        output_legend = Legend()
        for feature in legend.features.values():
            label = feature.label.lower()
            if label in true_legend_dict['pts']:
                stat_dict['pts'].append(label)
                feature.type = MapUnitType.POINT
                output_legend.features[label] = feature
            elif label in true_legend_dict['lines']:        
                stat_dict['lines'].append(label)
                feature.type = MapUnitType.LINE
                output_legend.features[label] = feature
            elif label in true_legend_dict['polygons']:
                stat_dict['polygons'].append(label)
                feature.type = MapUnitType.POLYGON
                output_legend.features[label] = feature
            else:
                if len(feature.label) < 5:
                    stat_dict['likely_poly'].append(label)
                    feature.type = MapUnitType.POLYGON
                    output_legend.features[label] = feature
                    continue
                else:
                    line_found = False
                    for line in true_legend_dict['lines']:
                        if line in label:
                            stat_dict['likely_lines'].append(line)
                            feature.type = MapUnitType.LINE
                            output_legend.features[label] = feature
                            line_found = True
                            break
                    if not line_found:
                        stat_dict['unknown'].append(label)
                        feature.type = MapUnitType.UNKNOWN
                        output_legend.features[label] = feature
        output_dict[map] = output_legend

    print(f'Test dictionary contains {len(stat_dict["pts"])} points, {len(stat_dict["lines"])} lines, {len(stat_dict["likely_lines"])} likely lines, {len(stat_dict["polygons"])} polygons, {len(stat_dict["likely_poly"])} likely polygons, and {len(stat_dict["unknown"])} unknowns')
    print(f'Test dictionary contains {len(set(stat_dict["pts"]))} unique points, {len(set(stat_dict["lines"]))} unique lines, {len(set(stat_dict["likely_lines"]))} unique likely lines, {len(set(stat_dict["polygons"]))} unique polygons, {len(set(stat_dict["likely_poly"]))} unique likely polygons, and {len(set(stat_dict["unknown"]))} unique unknowns')

    os.makedirs(output_dir, exist_ok=True)
    for map, legend in tqdm(output_dict.items()):
        saveSteupUpJson(os.path.join(output_dir, map + '.json'), legend)


if __name__ == '__main__':
    main()
