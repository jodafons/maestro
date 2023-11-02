import gradio as gr
from loguru import logger
from fastapi import FastAPI
from maestro.servers.web.tab_node import tab_node, setup_node


def create_context():
    context = {}
    setup_node(context)
    return context


with gr.Blocks(theme='freddyaboulton/test-blue') as demo:
    
    # create the context
    context = gr.State(create_context())
    text = gr.Markdown("Maestro")
    tab_node(context, name='Nodes')

    
app = FastAPI()
demo.queue(concurrency_count=10)
app = gr.mount_gradio_app(app,demo,path="/maestro")
    
if __name__ == "__main__":
    demo.queue(concurrency_count=10)
    demo.launch(server_name="0.0.0.0", server_port=7000, debug=True)