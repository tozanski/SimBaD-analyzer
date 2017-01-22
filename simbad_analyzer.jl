include("./configuration_reader.jl")
using ConfiguraionReader
using PyPlot

if length(ARGS) < 1
  error("not enought arguments")
end
property_names = ARGS
#min_val::Float64
#max_val::Float64
#resolution::Int64

min_val = Float64(0)
max_val = Float64(1)
resolution = Int64(100)

#if length(ARGS) >= 2
#  min_val = parse(Float64, ARGS[2])
#end
#
#if length(ARGS) >= 3
#  max_val = parse(Float64, ARGS[3])
#end
#
#if length(ARGS) >= 4
#  resolution = parse(Int64, ARGS[4])
#end

println(STDERR,"processing properties $property_names")

plot_time = Array{Float64,1}()
plot_data = Array{Float64,1}()

for (timestamp, data_header, data) in @async snapshot_reader(STDIN)
  println(STDERR, "read snapshot t=$timestamp, $(size(data)[1]) entries")
  
  property_indices = [ findfirst(data_header,property_name) for property_name in property_names]
  if countnz(property_indices) != length(property_indices)
    idx2 = findfirst(property_indices, 0)
    error("could not find property $(property_names[idx2]) in data header: $data_header")
  end
  property_cols = data[:,property_indices]
  data = 0
  
  edges = linspace(min_val,max_val,resolution+1)
  edges,counts = hist(property_cols, edges)
  property_cols = 0
  
  normalized_counts = counts ./ sum(counts,1)
  counts = 0

  push!(plot_time, timestamp)
  append!(plot_data, normalized_counts[:] )
end

nprops = length(property_names)
ntimes= length(plot_time)
new_dims = (resolution, nprops, ntimes)
plot_data = reshape(plot_data, new_dims)

i = 1
for property_name in property_names
  fig = figure(figsize=(20,10), dpi=300)
  data =  reshape(plot_data[:,i,:], (resolution,ntimes) )
  stackplot(plot_time, data)
  xlabel("time")
  ylabel("cell count")
  savefig("$property_name.png", dpi=300)
  i += 1
end