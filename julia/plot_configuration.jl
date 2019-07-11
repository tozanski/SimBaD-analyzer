__precompile__()

#module PlotConfiguration

#export plot_configuration

using DataFrames
using PyPlot


function plot_configuration(stream, color_column_name, len)
  println("coloring attribute: " * color_column_name)
  data = readtable(stream)
  
  x = Array{Float64,1}(data[:coord_0])
  y = Array{Float64,1}(data[:coord_1])
  c = Array{Float64,1}(data[symbol(color_column_name)]);  
  
  fig = figure(figsize=(len/5,len/5) )
  #fig = figure()
  subplot(111, aspect="equal")
  xlim([-len,len])

  scatter(x, y, c=c)
  colorbar()
  tight_layout()
  
  xlabel("x position")
  ylabel("y position")  
  title("parametric evolution: "*color_column_name )
  savefig("configuration_parametric_evolution_"*color_column_name, dpi=100)
  close(fig)
end


plot_configuration(STDIN,ARGS[1],200)

#end