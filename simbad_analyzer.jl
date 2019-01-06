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
mutation_parent_name = "parent.mutation.id"
parent_mutation_dump_path = "parent_mutation.csv"
#min_val::Float64
#max_val::Float64
#resolution::Int64

min_val = Float64(0)
max_val = Float64(1)
resolution = Int64(1000)

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

edges = range(min_val, stop=max_val, length=resolution+1)

println(stderr, "processing properties $property_names")

max_mutation = 0

function read_properties( stream, property_names, mutation_name, edges::AbstractVector{Float64} )
  plot_time = Vector{Float64}()
  plot_data = Vector{Float64}()
  mutation_data = Vector{ Any }()
  parent_data = Dict{Int,Int}()

  all_property_names = vcat(property_names,mutation_name)
  
  for (timestamp, configuration) in Channel( (ch)->produce_configurations(ch, stream, all_property_names) )
    #println(STDERR, "read snapshot t=$timestamp, $(size(configuration)) records")
  
    push!(plot_time, timestamp)

    for (index,property_name) in enumerate(property_names)
      column = configuration[:,index]

      histogram = fit(Histogram, column, edges, closed=:left)
      counts = histogram.weights
      normalized_counts = counts ./ sum(counts)
    
      append!(plot_data, normalized_counts )
    end
  
    mutation_index = length(property_names) + 1
    mutation_column = Vector{Int}(configuration[:,mutation_index])
    vals, counts = countuniquesorted(mutation_column)
    push!( mutation_data, (vals,counts) )
  end  
  
  nprops = length(property_names)
  ntimes= length(plot_time)
  new_dims = (resolution, nprops, ntimes)
  plot_data = reshape(plot_data, new_dims)
  
  return plot_time, plot_data, mutation_data
end

plot_time, plot_data, mutation_data = read_properties( stdin, property_names, mutation_name, edges)

println(stderr, "Plotting...")

# plot all the histogram plots
for (i,property_name) in enumerate(property_names)
  println(stderr, "property $property_name")
  fig = figure(figsize=(20,10), dpi=300)
  data =  reshape(plot_data[:,i,:], (:,length(plot_time)))
  stackplot(plot_time, data)
  xlabel("time")
  ylabel("cell count")
  savefig("$property_name.png", dpi=300)
end

println(stderr, "Mutations..")
all_mutations = mapreduce( (x)->x[1], union, mutation_data )
assert(issorted(all_mutations))
num_mutations = length(all_mutations)
println(STDERR, "Counted $num_mutations different mutations")

#mutation_plot_data = Matrix{Float64}( nmutations, ntimes)

println(STDERR, "Processing mutations...")

is_noise_mutation = trues(num_mutations)

for (time_idx,(vals,counts)) in enumerate(mutation_data)
  indices = [searchsortedfirst(all_mutations,val) for val in vals]
  is_large = counts .> 10
  is_noise_mutation[indices[is_large]] = false
  #mutation_plot_data[indices, time_idx] = counts
end

num_noise_mutations = sum(is_noise_mutation)
num_large_mutations = num_mutations - num_noise_mutations

println(STDERR, "Detected $num_noise_mutations noise mutations and $num_large_mutations large mutations")

large_mutations_plot_data = zeros(Int64,num_large_mutations, length(plot_time))
noise_mutations_counts = zeros(Int64, length(plot_time))

large_mutations = all_mutations[.!is_noise_mutation]

for (time_idx,(ids,counts)) in enumerate(mutation_data)
  for (id,count) in zip(ids,counts)
    idx = findfirst(large_mutations, id)
    if 0 == idx
      noise_mutations_counts[time_idx] += count
    else
      large_mutations_plot_data[idx, time_idx] += count
    end
  end
end



println(STDERR, "Loading dump file..")
parent_list = readcsv("parent_mutations.csv",Int)
parent_dict = Dict{Int,Int}(zip(parent_list[:,1], parent_list[:,2]))

function build_ancestry(mutation_id, parent_dict)
  id = mutation_id
  ancestry = Vector{Int}()
  while 0 != id
    push!(ancestry, id)
    id = parent_dict[id]
  end 
  return ancestry
end

# create list of tuples, a tuple for each mutation
ancestry = [Tuple(flipdim(build_ancestry(x,parent_dict),1) ) for x in large_mutations ]
#println.(ancestry)
permutation = sortperm(ancestry)
#println(permutation)
large_mutations_plot_data = large_mutations_plot_data[permutation,:]
println.(ancestry[permutation])

mutation_counts = vcat(noise_mutations_counts', large_mutations_plot_data, )
mutation_plot_data = mutation_counts ./ sum(mutation_counts,1)


println(STDERR, "Plotting")
fig = figure(figsize=(20,10), dpi=300)
stackplot(plot_time, mutation_plot_data)
xlabel("time")
ylabel("mutation prevalence")
savefig("mutations.png", dpi=300)










