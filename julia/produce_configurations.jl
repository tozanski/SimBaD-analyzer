using CSV

function read_configuration(stream::IO,
                            headers::Vector{String}, 
                            buffer::IOBuffer = IOBuffer(), 
                            split_line::Vector{CSV.RawField} = Vector{CSV.RawField}(0),
                            indices::Vector{Int} = Vector{Int}(length(headers)),
                            values::Vector{Float64} = Vector{Float64}(0);
                            delimiter = ',', 
                            quotation = '"', 
                            escape = '\\') 
                                                    
  has_headers = false
  empty!(values)
  
  while(!eof(stream))
    split_line = CSV.readsplitline!(split_line, stream, delimiter, quotation, escape, buffer)
    
    if 0 == length(split_line) # eof?
      break
    end
    
    if 1 == length(split_line)  # spacing or time=xxx
      field = split_line[1].value
      if 0 == length(field) # spacing
        break
      end
      # time line
      #values = Vector{Float64}(0)
      #has_headers = false
      continue
    end
    
    if !has_headers
      empty!(indices)                                       # clear index table
      sizehint!(indices, length(headers))
      
      for header in headers
        index = find( (x)->x.value==header, split_line)[1]  # there should be just one
        if 1 != length(index)
          error("could not find name `$header` in header or found too many")
        end 
        push!(indices, index)                               # fill index table
      end
      has_headers = true  
      continue
    end
    
    for index in indices
      string_value = split_line[index].value
      value = parse(Float64,string_value)
      push!(values, value)
    end
  end
  
  result = reshape(values, (length(headers),:))
  
end


function produce_configurations(ch::Channel, stream::IO, headers; delimiter = ',', quotation = '"', escape = '\\'
)

  buffer = IOBuffer()
  split_line = Vector{CSV.RawField}(0)
  indices = Vector{Int}(0)
  
  

  has_headers = false
  while !eof(stream)
    timestamp_line = readline( stream )
    if timestamp_line[1:7] != "time = "
      error("timestamp not found")
    end
    timestamp = parse(Float64, timestamp_line[8:end])
    
    values = Vector{Float64}(0)
    read_result = read_configuration( stream, headers, buffer, split_line, indices, values,
                                      delimiter = delimiter, quotation = quotation,  escape = escape)
    configuration = transpose(read_result) 
    put!(ch, (timestamp,configuration) )
  end
end
