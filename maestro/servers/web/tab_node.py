
__all__ = ["tab_node", "setup_node"]

import gradio as gr
from maestro import schemas
from pprint import pprint

pilot_host = "http://caloba91.lps.ufrj.br:5000"


def setup_node( context ):
    context['system_info'] = {}
    return context


def get_size(bytes, suffix="B"):
    factor = 1024
    for unit in ["", "K", "M", "G", "T", "P"]:
        if bytes < factor:
            return f"{bytes:.2f}{unit}{suffix}"
        bytes /= factor


def update_button_action(context, host_select):
    api = schemas.client(pilot_host, "pilot")
    answer = api.try_request("system_info" , method="get")
    if answer.status:
        context['system_info'] = {o['node']:o for o in answer.metadata.values()}
    choices = list(context['system_info'].keys())
    return context, gr.Dropdown.update(choices=choices)



def select_host_action(context, host_select, device_select):

    info         = context['system_info'][host_select]
    host         = gr.Textbox.update(info['host'])
    cpu_family   = gr.Textbox.update(info['cpu']['processor'])
    total_cores  = gr.Textbox.update(info['cpu']['cpu'])
    usage_cores  = gr.Textbox.update(info['cpu']['cpu_usage'])
    total_memory = gr.Textbox.update(get_size(info['memory']['memory_total']))
    usage_memory = gr.Textbox.update(info['memory']['memory_usage'])

    gpu_family = ""; gpu_total_memory = ""; gpu_usage_memory = ""

    available_devices = ['cpu']
    for gpu in info['gpu']:
        device = gpu['id']
        gpu_family += f"{device}:{gpu['name']},"
        gpu_total_memory += f"{device}:{get_size(gpu['memory_total'])},"
        gpu_usage_memory += f"{device}:{gpu['memory_usage']},"
        available_devices.append(f'gpu:{device}')

    gpu_family        = gr.Textbox.update(gpu_family.strip(','))
    gpu_total_memory  = gr.Textbox.update(gpu_total_memory.strip(','))
    gpu_usage_memory  = gr.Textbox.update(gpu_usage_memory.strip(','))

    total_slots       = gr.Textbox.update(info['consumer']['size'])
    allocated_slots       = gr.Textbox.update(info['consumer']['allocated'])
    

    return host, cpu_family, total_cores, usage_cores, total_memory, usage_memory, gpu_family,\
           gpu_total_memory, gpu_usage_memory, total_slots, allocated_slots, gr.Dropdown.update(choices=available_devices)



def tab_node(context, name='Nodes'):

    with gr.Tab(name):

        with gr.Row():
            with gr.Group():
                update_button = gr.Button("Update")

        with gr.Row():
            with gr.Group():
                with gr.Column():
                    with gr.Row():                
                        host_select = gr.Dropdown([], info="Select the node", show_label=False)
                    with gr.Row():                
                        host        = gr.Textbox(label="Local host")
                    with gr.Row():
                        cpu_family  = gr.Textbox(label="CPU Family")
                    with gr.Row():
                        total_cores  = gr.Textbox(label="CPU Cores")
                        usage_cores  = gr.Textbox(label="CPU Usage")
                    with gr.Row():
                        total_memory = gr.Textbox(label="Memory")
                        usage_memory = gr.Textbox(label="Memory Usage")
                    with gr.Row():
                        gpu_family = gr.Textbox(label="GPU Name")
                    with gr.Row():
                        gpu_total_memory = gr.Textbox(label="GPU Memory")  
                        gpu_usage_memory = gr.Textbox(label="GPU Memory Usage")  
                    update_host_button = gr.Button("Updata")

            #with gr.Group():
            with gr.Column():
                with gr.Group():
                    with gr.Row():
                        queue = gr.Dropdown(label="Queues", choices=['gpu','cpu'])
                    with gr.Row():
                        devices = gr.Dropdown(label="Queues")

                    with gr.Row():
                        total_slots     = gr.Textbox(label="Total Slots")  
                        allocated_slots = gr.Textbox(label="Allocated Slots")  
                with gr.Row():
                    down_botton = gr.Button("Down",min_width=40)
                    maxjobs = gr.Textbox(min_width=30, show_label=False)
                    up_botton = gr.Button("Up",min_width=40)
                with gr.Row():
                    submit_button = gr.Button("Submit")


        panel = [host, cpu_family, total_cores, usage_cores, total_memory, usage_memory,
                 gpu_family, gpu_total_memory, gpu_usage_memory, total_slots, allocated_slots, devices]

        update_button.click( update_button_action, inputs=[context, host_select], outputs=[context, host_select] )
        host_select.change( select_host_action, inputs=[context, host_select, devices], outputs=panel )
