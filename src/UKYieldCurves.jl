module UKYieldCurves

using ZipFile, XLSX, DataFrames, Dates, CSV, Plots

function downloadLatestData(directory;latestonly=false)
    urlstem = raw"https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves"
    filenamelatestdata = "latest-yield-curve-data.zip"
    filenamenominaldata = "glcnominalddata.zip"
    filenameinflationdata = "glcinflationddata.zip"
    if latestonly
        files = [filenamelatestdata]
    else
        files = [filenamelatestdata,filenameinflationdata,filenamenominaldata]
    end

    for file in files
        download(urlstem*"/"*file,joinpath(directory,file))
        #uses 7 zip
        run(`7z e $(joinpath(directory,file)) -y -o$(directory)`)
    end
end

function gatherdata(; directory="", outdirectory="",fetch=false, saveme=true, latestonly=false)
    fetch && downloadLatestData(directory,latestonly=latestonly)
    dfs = [DataFrame() DataFrame()
            DataFrame() DataFrame()]
    categories = ["GLC Inflation","GLC Nominal"]
    for file in readdir(directory,join=true)
        for (i, category) in enumerate(categories)
            if occursin(category,file) && (!latestonly || occursin(r"(?:current month|present)",file))
                XLSX.openxlsx(file) do xf
                    for sheetname in XLSX.sheetnames(xf)
                        for (j, ratetype) in enumerate(["spot curve","fwd curve"])
                            if occursin(ratetype,sheetname)
                                dfs[i,j] = vcat(dfs[i,j],DataFrame(XLSX.readtable(file, sheetname,first_row=4)...),cols=:union)
                            end
                        end
                    end
                end
            end
        end
    end
    rename!.(dfs,"years:"=>"date")
    sort!.(dfs,"date",rev=true)
    filter!.(Ref(:date=>x->.!ismissing.(x)),dfs)
    if saveme
        savedata(outdirectory,dfs)
    else
        dfs
    end
end

function savedata(directory,dfs;dividingyear=2016)

    #the series corresponding to first dimension of dfs
    seriestype = ["BoE Implied RPI","uknom"]

    #the suffixes corresponding to the second dimension of dfs
    suffixes = [""," FWD"]

    for (i,series) in enumerate(seriestype)
        for (j, suffix) in enumerate(suffixes)
            dateval = dfs[i,j][:,:date]
            dfs[i,j].date .= Dates.format.(dfs[i,j].date,"d/m/Y")
            CSV.write(joinpath(addindirectory,series * suffix * ".csv"),dfs[i,j][year.(dateval).<dividingyear,:],newline="\r\n")
            for y in dividingyear:year(now())
                file = series * suffix * " " * string(y) * ".csv"
                CSV.write(joinpath(addindirectory,file),dfs[i,j][year.(dateval).==y,:],newline="\r\n")
            end
        end
    end
end

function getlastdate(directory)
    files = readdir(directory,join=true)
    filter!(x->occursin(r"uknom \d",x),files)
    sort!(files,rev=true)
    rows = CSV.Rows(files[1],limit=1)
    for row in rows
        return Date(row[1],dateformat"d/m/Y")
    end
end

function getBoE(directory, series;date=nothing,duration=nothing)
    files = readdir(directory,join=true)
    filter!(x->(occursin(Regex(series*"( \\d{4})?.csv"),x)),files)
    df = DataFrame()
    for file in files
        df = vcat(df,CSV.read(file,DataFrame))
    end
    df.date = Date.(df.date,"d/m/Y")
    sort!(df,:date)
end 

function getrow(df,d)
    d = maximum(filter(<(d),df.date))
    row = df[df.date.==d,Not(:date)]
    if size(row,1) > 0
        return [row[1,name] for name in names(row)]
    else
        return []
    end
end

function plotBoE(series, datestoplot)
    df = getBoE(series)
    plot(names(df)[2:end],getrow.(Ref(df),datestoplot),title=series,label=reshape(datestoplot,1,:),linewidth=3,thickness_scaling = 1)
end

function animateBoE(series, range; folder = "")
    df = getBoE(series)
    anim = Animation()
    axismax = ceil(maximum(skipmissing(vcat(getrow.(Ref(df),range)...))))
    axismin = floor(minimum(skipmissing(vcat(getrow.(Ref(df),range)...))))
    for d in range
        plot(names(df)[2:end],getrow(df,d),title=series,label=d, ylims = (axismin,axismax))  # plot new regression line
        frame(anim)
    end
    gif(anim, joinpath(folder,series*".gif"), fps=6)
end

end # module
