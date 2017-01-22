#__precompile__()
module ConfiguraionReader

import Base.read
import Base.eof
import Base.close

export SplitInput, read, eof, close, proceed, lookup, read_configuration
export snapshot_reader

type SplitInput <: IO
  istream::IO
  line
  buffer::IOBuffer
  split_fun::Function

  SplitInput(istream::IO, line, buffer::IOBuffer, split_fun) = new(istream, line,buffer,split_fun)
  SplitInput(istream::IO, line,split_fun) = SplitInput(istream, line, IOBuffer(line),split_fun)
  SplitInput(istream::IO,split_fun) = SplitInput(istream, readline(istream), split_fun)
end

function read(lbio::SplitInput, dtype::Type{UInt8})
  ret = read(lbio.buffer,dtype)
  if eof(lbio.buffer)
    lbio.line = readline(lbio.istream)
    lbio.buffer = IOBuffer(lbio.line)
  end
  return ret
end

function eof(lbio::SplitInput)
  return lbio.split_fun(lbio.line) || (eof(lbio.buffer) &&  eof(lbio.istream))
end

function proceed(lbio::SplitInput)
  lbio.line = readline(lbio.istream)
  lbio.buffer = IOBuffer(lbio.line)
end

function lookup(lbio::SplitInput)
  return lbio.line
end

function close(lbio::SplitInput)

end

function parse_timestamp_header(line)
  minimal_len = length("time = 0")
  if length(line) <= minimal_len
    error("could not parse timestamp: ``$line''")
  end
  return parse(Float64,line[minimal_len:end])
end

function read_configuration(istream::IO)
  arr = readcsv(istream)
  #TODO: change output format
  #arr = arr[:,1:end-1] #remove last column
  header = Array{AbstractString,1}(arr[1,:][:])
  data = arr = Array{Float64,2}(arr[2:end,:])
  return header,data
end

function snapshot_reader(istream::IO)
  time_header = readline(istream)
  split_on_time_header(line) = length(line) >= 4 && "time" == line[1:4]
  while !eof(istream)
    splitted_istream = SplitInput(istream, split_on_time_header)
    data_header, data = read_configuration(splitted_istream)
    timestamp = parse_timestamp_header(time_header)
    produce(timestamp,data_header,data)
    time_header = lookup(splitted_istream)
  end
end

end
