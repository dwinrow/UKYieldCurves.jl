module UKYieldCurves

    using ZipFile, XLSX, DataFrames, Dates, CSV, Downloads

    export downloadLatestData, gatherdata, getBoE, getrow, getlastdate

    defaultdirectory = joinpath("data","yieldcurves")
    defaultdownloadsdirectory = joinpath("data","yieldcurves","downloaded")

    """
        downloadLatestData(directory;latestonly=false)
    Download the yield curve data zips and unzip their contents to the directory given
    """
    function downloadLatestData(;directory=defaultdownloadsdirectory,latestonly=false)
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
            url = urlstem*"/"*file
            @info "Downloading $url"
            zipbuff = Downloads.download(url,IOBuffer())
            r = ZipFile.Reader(zipbuff)
            for f in r.files
                write(joinpath(directory,f.name),f)
            end
        end
    end

    """
        gatherdata(; directory="", outdirectory="",fetch=false, saveme=true, latestonly=false
                    categories = ["GLC Inflation","GLC Nominal"],
                    curveshtnames = ["spot curve","fwd curve"]
        )
    Read the downloaded yield curve into a matrix of DataFrames
        - the first dimension being the workbook name specified by `categories`
        - the second dimension being the curve type sheet name specified by `curveshtnames`
        `fetch` - if true, downloads the files
        `saveme` - if true, saves the DataFrames as csvs to `outdirectory`
        `latestonly` - if true restricts scope to the latest month's data
    """
    function gatherdata(; directory=defaultdownloadsdirectory, outdirectory=defaultdirectory,fetch=false, saveme=true, latestonly=false,
                            categories = ["GLC Inflation","GLC Nominal"],
                            curveshtnames = ["spot curve","fwd curve"],
                            dividingyear = year(today())+1
                        )
        fetch && downloadLatestData(;directory=directory,latestonly=latestonly)
        dfs = [DataFrame() DataFrame()
                DataFrame() DataFrame()]
        for file in readdir(directory,join=true)
            for (i, category) in enumerate(categories)
                if occursin(category,file) && (!latestonly || occursin(r"(?:current month|present)",file))
                    XLSX.openxlsx(file) do xf
                        for sheetname in XLSX.sheetnames(xf)
                            for (j, ratetype) in enumerate(curveshtnames)
                                if occursin(ratetype,sheetname)
                                    dfs[i,j] = vcat(dfs[i,j],DataFrame(XLSX.readtable(file, sheetname,first_row=4)),cols=:union)
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
            savedata(outdirectory,dfs;dividingyear=dividingyear)
        end
        dfs
    end

    """
        savedata(directory,dfs;dividingyear=2016,
                seriestype = ["BoE Implied RPI","uknom"], 
                suffixes = [""," FWD"]
        )
    Saves the matrix of DataFrames formed by `gatherdata`

        - `seriestype` the name of the file by the first dimension
        - `suffixes` the suffix to add to the seriestype by the second dimension
    """
    function savedata(directory,dfs;dividingyear=year(today())+1,
        seriestype = ["BoE Implied RPI","uknom"],
        suffixes = [""," FWD"]
        )

        for (i,series) in enumerate(seriestype)
            for (j, suffix) in enumerate(suffixes)
                dateval = dfs[i,j][:,:date]
                dfs[i,j].date .= Dates.format.(dfs[i,j].date,"Y-m-d")
                #save all data before the dividing year as one file
                CSV.write(joinpath(directory,series * suffix * ".csv"),dfs[i,j][year.(dateval).<dividingyear,:],newline="\r\n")
                #save data after the dividing year as different files for each year to allow faster access
                for y in dividingyear:year(now())
                    file = series * suffix * " " * string(y) * ".csv"
                    CSV.write(joinpath(directory,file),dfs[i,j][year.(dateval).==y,:],newline="\r\n")
                end
            end
        end
    end

    """
        getlastdate(directory)
    Gets the latest date of the uknom files in the directory
    """
    function getlastdate(directory=defaultdirectory)
        files = readdir(directory,join=true)
        filter!(x->occursin(r"uknom",x),files)
        sort!(files,rev=true)
        file = files[length(files) == 1 ? 1 : 2]
        for row in CSV.Rows(file,limit=1)
            date = row[1]
            if occursin('/',date)
                return Date(date,dateformat"d/m/Y")
            else
                return Date(date)
            end
        end
    end

    """
        getBoE(directory, series;date=nothing,duration=nothing)
    Load all of the BoE csv files in `directory` into a DataFrame
    """
    function getBoE(directory=defaultdirectory, series="uknom")
        files = readdir(directory,join=true)
        filter!(x->(occursin(Regex(series*"( \\d{4})?.csv"),x)),files)
        df = DataFrame()
        for file in files
            df = vcat(df,CSV.read(file,DataFrame))
        end
        #df.date = Date.(df.date,"d/m/Y")
        sort!(df,:date)
    end 

    """
        getrow(df,d)
    Get a row of a DataFrame by date `d`
    """
    function getrow(df=getBoE(),d=today())
        d = maximum(filter(<(d),df.date))
        row = df[df.date.==d,Not(:date)]
        if size(row,1) > 0
            return [row[1,name] for name in names(row)]
        else
            return []
        end
    end

end # module
