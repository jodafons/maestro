import gradio as gr
import pandas as pd


def translate(text):
    return pipe(text)[0]["translation_text"]

complete = gr.HighlightedText(label="Diff",
        combine_adjacent=True,
        show_legend=True,
        color_map={"+": "red", "-": "green"})

cols = ["registered","assigned","testing","running","broken","kill","killed","failed","completed","status"]
vals = [0,0,0,5,0,0,0,5,0,'running']
df = pd.DataFrame( {key:[vals[i]] for i, key in enumerate(cols)})

with gr.Blocks(theme='freddyaboulton/test-blue') as demo:
    
    text = gr.Markdown("LPS")
    with gr.Tab("Queue"):
        with gr.Row():
            task_dropdown = gr.Dropdown( ["ran", "swam", "ate", "slept"], value=[], multiselect=True, label="Task", info="Select the task.")
        queue_table   = gr.Dataframe(df, headers=cols, visible=True )
        #pass
    with gr.Tab("Submission"):
        pass
    with gr.Tab("Reconstruction"):
        pass
    with gr.Tab("Nodes"):
        pass

    
    






if __name__ == "__main__":
    demo.launch(share=False, server_name="0.0.0.0")#, server_port=7860)