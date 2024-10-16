import os
import json
from pynwb import NWBFile, NWBHDF5IO
import numpy as np
import randomaccessbuffer as rab
import matplotlib.pyplot as plt

from hdmf.common.hierarchicaltable import to_hierarchical_dataframe, flatten_column_index


def nwb2png(nwb_path, nwb, png_dir):
    filename = nwb_path.split("/")[-1].split(".")[0]

    trials_df = to_hierarchical_dataframe(nwb.icephys_repetitions).reset_index()
    trials_df = flatten_column_index(trials_df, max_levels=2)
    stimuli = list(set((trials_df[('sequential_recordings', 'stimulus_type')])))
    for stimulus in stimuli:
        if isinstance(stimulus, bytes):
            stimulus_str = stimulus.decode()
        else:
            stimulus_str = stimulus
        repetitions = set(trials_df[(trials_df[('sequential_recordings', 'stimulus_type')] == stimulus)][('repetitions', 'id')])
        for repetition in repetitions:
            trials_response = trials_df[(trials_df[('sequential_recordings', 'stimulus_type')] == stimulus)][
                (trials_df[('repetitions', 'id')] == repetition)][('responses', 'response')]
            trials_stimulus = trials_df[(trials_df[('sequential_recordings', 'stimulus_type')] == stimulus)][
                (trials_df[('repetitions', 'id')] == repetition)][('stimuli', 'stimulus')]
            response_sweep_count = 0
            stimulation_sweep_count = 0
            for item in trials_response.iteritems():
                if item[1][2]:
                    response_sweep_count += 1
                    data = (item[1][2].data * item[1][2].conversion) * 1000  # to get mV
                    rate = int(item[1][2].rate)
                    x = (np.arange(0, len(data)/rate, 1/rate)) * 1000  # to get ms
                    color = "k"
                    title = "response"
                    plt.plot(x, data, color)
                    plt.ylabel("Membrane potential (mV)")
                    plt.xlabel("Time (ms)")
                    plt.title(f"""Stimulus: {stimulus_str}
Response traces
Repetition n° {repetition}: {response_sweep_count} sweep(s)""", loc="center")
            out = f"{png_dir}/{filename}__{stimulus_str}__{title}__{repetition}.png"
            plt.savefig(out, dpi=300, bbox_inches='tight', facecolor='w')
            #         plt.show()
            plt.close()
            for item in trials_stimulus.iteritems():
                if item[1][2]:
                    stimulation_sweep_count += 1
                    data = (item[1][2].data * item[1][2].conversion) * 1000000000000  # to get pA
                    rate = int(item[1][2].rate)
                    x = (np.arange(0, len(data)/rate, 1/rate)) * 1000  # to get ms
                    color = "r"
                    title = "stimulus"
                    plt.plot(x, data, color)
                    plt.ylabel("Current (pA)")
                    plt.xlabel("Time (ms)")
                    plt.title(f"""Stimulus: {stimulus_str}
Stimulation traces
Repetition n° {repetition}: {stimulation_sweep_count} sweep(s)""", loc="center")
            out = f"{png_dir}/{filename}__{stimulus_str}__{title}__{repetition}.png"
            plt.savefig(out, dpi=300, bbox_inches='tight', facecolor='w')
            plt.close()


# The below function was provided by Jonathan Lurie here: https://bbpgitlab.epfl.ch/dke/kgforge-mappers/-/blob/1773e3eb1c3227c745278cd4e3b95bd648ef2d00/kgmappers/newNwbToRab.py
def nwb2rab(nwb, rab_path):
    rabuff = rab.RandomAccessBuffer()
    index = {}

    trials_df = to_hierarchical_dataframe(nwb.icephys_repetitions).reset_index()
    trials_df = flatten_column_index(trials_df, max_levels=2)

    stimulus_types = list(set((trials_df[('sequential_recordings', 'stimulus_type')])))

    for stimulus in stimulus_types:
        if isinstance(stimulus, bytes):
            stimulus_str = stimulus.decode()
        else:
            stimulus_str = stimulus

        subset_df = trials_df[(trials_df[('sequential_recordings', 'stimulus_type')] == stimulus)]
        none_free_df = subset_df.drop(subset_df[subset_df[('responses', 'response')] == (None, None, None)].index)
        none_free_df = none_free_df.drop(none_free_df[none_free_df[('stimuli', 'stimulus')] == (None, None, None)].index)
        dt = 1 / none_free_df[('responses', 'response')].iloc[0][2].rate
        dur = len(none_free_df[('responses', 'response')].iloc[0][2].data[:]) * (1 / none_free_df[('responses', 'response')].iloc[0][2].rate)
        t_unit = none_free_df[('responses', 'response')].iloc[0][2].starting_time_unit
        i_unit = none_free_df[('stimuli', 'stimulus')].iloc[0][2].unit
        v_unit = none_free_df[('responses', 'response')].iloc[0][2].unit
        name = f"{nwb.session_id}_{stimulus_str}"

        # Adding the repetition and to what sweep they correspond
        repetition_sweeps = {}
        # for repetition in set(subset_df["repetition"]):
        for repetition in set(subset_df[('repetitions', 'id')]):
            list_of_sweeps = []
            for i in range(len(subset_df[(subset_df[('repetitions', 'id')] == repetition)][('responses', 'response')])):
                sweep_id = str(subset_df[(subset_df[('repetitions', 'id')] == repetition)][('intracellular_recordings', 'id')].iloc[i])
                list_of_sweeps.append(sweep_id)

            repetition_sweeps[str(repetition)] = {
              "sweeps": list_of_sweeps
            }

        json_file = {
            "values": list()
        }

        for repetition in set(subset_df[('repetitions', 'id')]):
            for i in range(len(subset_df[(subset_df[('repetitions', 'id')] == repetition)][('responses', 'response')])):
                if subset_df[(subset_df[('repetitions', 'id')] == repetition)][('stimuli', 'stimulus')].iloc[i][2] is not None:
                    current = list(map(float, (subset_df[(subset_df[('repetitions', 'id')] == repetition)][
                        ('stimuli', 'stimulus')].iloc[i][2].data * subset_df[(subset_df[('repetitions', 'id')] == repetition)][
                            ('stimuli', 'stimulus')].iloc[i][2].conversion)))
                else:
                    current = None
                if subset_df[(subset_df[('repetitions', 'id')] == repetition)][
                             ('responses', 'response')].iloc[i][2] is not None:
                    voltage = list(map(float, (subset_df[(subset_df[('repetitions', 'id')] == repetition)][
                        ('responses', 'response')].iloc[i][2].data * subset_df[(subset_df[('repetitions', 'id')] == repetition)][
                            ('responses', 'response')].iloc[i][2].conversion)))
                else:
                    voltage = None
                sweep = str(subset_df[(subset_df[('repetitions', 'id')] == repetition)][('intracellular_recordings', 'id')].iloc[i])

                json_file["values"].append({
                    "i": current,
                    "v": voltage,
                    "sweep": sweep
                })
        
        json_file["values"] = {value["sweep"]: {"i": value["i"], "v": value["v"]} for value in json_file["values"]}

        index[stimulus_str] = {
            "dt": float(dt),
            "dur": float(dur),
            "t_unit": t_unit,
            "i_unit": i_unit,
            "v_unit": v_unit,
            "name": name,
            "repetitions": repetition_sweeps,
            "sweeps": {}
        }

        for sweep_id in json_file["values"]:
            index[stimulus_str]["sweeps"][sweep_id] = {}

            # measure_sub_sub_id are going to be "i" and "v"
            for measure_sub_sub_id in json_file["values"][sweep_id]:
                numerical_dataset_id = "{} {} {}".format(stimulus_str, sweep_id, measure_sub_sub_id)
                index[stimulus_str]["sweeps"][sweep_id][measure_sub_sub_id] = numerical_dataset_id

                # We take the buffer of numbers, make it an efficient numpy array
                numbers = np.array(json_file["values"][sweep_id][measure_sub_sub_id], dtype="float64")

                # and then add it to the RandomAccessBuffer instance
                rabuff.addDataset(numerical_dataset_id, data=numbers, compress="gzip")

    # Adding the index
    rabuff.addDataset("index",
    data = index,
    metadata = {
      "session_id": nwb.session_id
    },
    compress="gzip")

    # Writing the RAB file on disk
    rabuff.write(rab_path)
    

def get_nwb_object(nwb_path):
    nwb = NWBHDF5IO(nwb_path, 'r', load_namespaces = True).read()
    return nwb


def generate(nwb_path, rab_path, png_dir):
    nwb = get_nwb_object(nwb_path=nwb_path)
    nwb2png(nwb_path, nwb, png_dir)
    nwb2rab(nwb, rab_path)
    