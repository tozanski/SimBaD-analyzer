import igraph
import pyarrow.parquet as pq
import numpy as np
import fire

def arrow_to_graph(path):
    df = pq.read_table(path).to_pandas()#.sort_values("mutationId")
    vertex_mapping = dict( (mutationId, vertexId) for vertexId, mutationId in enumerate(df["mutationId"]))
    is_root = np.array(df["ancestors"].map(lambda x: len(x))) == 1
    graph = igraph.Graph()
    graph.add_vertices( len(vertex_mapping))
    links = list(zip(
        (vertex_mapping[x[-2]] for x in df["ancestors"][~is_root]),
        (vertex_mapping[x] for x in df["mutationId"][~is_root])
    ))
    graph.add_edges(links)
    graph.vs["mutationId"] = df["mutationId"]
    graph.vs["type_count"] = df["typeCount"]
    graph.vs["mutation_count"] = df["mutationCount"]
    graph.vs[vertex_mapping[1]]
    roots = np.where(is_root)
    return graph, roots[0]
    
def make_visual_style(graph):
    visual_style = {}
    visual_style["vertex_size"] = 10*np.log2(np.array(graph.vs["type_count"])+1)+1
    visual_style["vertex_label"] = graph.vs["mutationId"]
    return visual_style
    
def mutation_tree_plot(data_path, output_path):
    g, roots = arrow_to_graph(data_path)
    roots
    g_layout = g.layout_reingold_tilford(root=[int(x) for x in roots])
    visual_style = make_visual_style(g)

    _ = igraph.plot(g, 
                output_path, 
                layout=g_layout, 
                inline=False, 
                bbox=(3840,1080), 
                **visual_style)
    
if __name__ == "__main__":
    fire.Fire(mutation_tree_plot)