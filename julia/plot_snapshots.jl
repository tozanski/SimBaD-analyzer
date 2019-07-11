using PyPlot

function plot_snapshots()
  time_header_regex = r"time = ([-+]?[0-9]\.?[0-9]*)"

  t = NaN
  t::Float64

  frameno = 0
  frameno::Int64
  
  framedir = tempdir() * "/frames/"
  mkpath(framedir)
  println("storing frames in $framedir")

  configuration = Array{Float64,1}()
  configuration::Array{Float64,1}
  
  function plot_configuration(time, frameno, config)
    fig = figure()
    plot(config[:,1], config[:,2], ".")
    axis( [-10, 10, -10, 10] )
    xlabel("x")
    ylabel("y")
    title("evolution for t=$time")
    
    filename = framedir * lpad(frameno,5,0)
    savefig(filename)
    close(fig)
  end


  time_header_regex = r"time = ([-+]?[0-9]\.?[0-9]*)"
  #particle_regex = r"([-+]?[0-9]\.?[0-9]*) ([-+]?[0-9]\.?[0-9]*)" 
  for line in eachline(STDIN)
    if ismatch(time_header_regex, line)
      
      
      if 0 < length(configuration)
        nparticles = Int(length(configuration)/2)
        reshaped = reshape( configuration, nparticles, 2);
        plot_configuration( t, frameno, reshaped )
        configuration = Array{Float64,1}()
      end
    
      m = match(time_header_regex, line)
      t = parse(Float64, m[1])
      frameno = frameno + 1
      continue
    end
    
    pos_str_arr = split(line)
    #m = match(particle_regex, line)
    
    xpos = parse(Float64, pos_str_arr[1])
    ypos = parse(Float64, pos_str_arr[2])
    
    push!(configuration, xpos)
    push!(configuration, ypos)

  end
  run(`ffmpeg -framerate 30 -i $framedir%05d.png -c:v libx264 -r 30 -tune animation -pix_fmt yuv420p out.mp4`)
end

plot_snapshots()
