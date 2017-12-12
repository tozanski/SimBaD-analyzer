include("./produce_configurations.jl")

using PyPlot
using TomekUtils
using StatsBase

#if length(ARGS) < 1
#  error("not enought arguments")
#end
property_names = [
  "birth.efficiency",
  "birth.resistance",
  "lifespan.efficiency",
  "lifespan.resistance",
  "success.efficiency",
  "success.resistance"
  ]

mutation_name = "mutation.id"

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

plot_time = Vector{Float64}()
plot_data = Vector{Float64}()

mutation_data = Vector{ Any }()
max_mutation = 0
edges = linspace(min_val,max_val,resolution+1)

all_property_names = vcat(property_names,mutation_name)



#for (timestamp, data_header, data) in @async snapshot_reader(STDIN)
for (timestamp, configuration) in Channel( (ch)->produce_configurations(ch, STDIN, all_property_names) )
  println(STDERR, "read snapshot t=$timestamp, $(size(configuration)) records")
  
  push!(plot_time, timestamp)

  for (index,property_name) in enumerate(property_names)
    column = configuration[:,index]

    histogram = fit(Histogram, column, edges, closed=:left)
    counts = histogram.weights
    normalized_counts = counts ./ sum(counts)
    
    append!(plot_data, normalized_counts )
  end
  
  mutation_index = length(property_names) + 1
  mutation_column = configuration[:,mutation_index]
  vals, counts = countuniquesorted(mutation_column)
  push!( mutation_data, (vals,counts) )

  
  
end

nprops = length(property_names)
ntimes= length(plot_time)
new_dims = (resolution, nprops, ntimes)
plot_data = reshape(plot_data, new_dims)
println(STDERR,"Plotting...")
i = 1
#for property_name in property_names
#  println(STDERR, "property $property_name")
#  fig = figure(figsize=(20,10), dpi=300)
#  data =  reshape(plot_data[:,i,:], (resolution,ntimes) )
#  stackplot(plot_time, data)
#  xlabel("time")
#  ylabel("cell count")
#  savefig("$property_name.png", dpi=300)
#  i += 1
#end

println(STDERR, "Mutations..")
all_mutations = mapreduce( (x)->x[1], union, mutation_data )
assert(issorted(all_mutations))
nmutations = length(all_mutations)
println(STDERR, "Detected $nmutations different, trying to allocate plot data")

mutation_plot_data = Matrix{Float64}( nmutations, ntimes)

println(STDERR, "Processing mutations...")
for (time_idx,(vals,counts)) in enumerate(mutation_data)
  indices = [findfirst(all_mutations,val) for val in vals]
  mutation_plot_data[indices, time_idx] = counts / sum(counts)
end
println(STDERR, "Plotting")
fig = figure(figsize=(20,10), dpi=300)
stackplot(plot_time, mutation_plot_data)
xlabel("time")
ylabel("mutation prevalence")
savefig("mutations.png", dpi=300)










