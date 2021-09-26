FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["TTQ.Batch/TTQ.Batch.csproj", "TTQ.Batch/"]
COPY ["TTQ.Manager/TTQ.Manager.csproj", "TTQ.Manager/"]
RUN dotnet restore "TTQ.Batch/TTQ.Batch.csproj"
COPY . .
WORKDIR "/src/TTQ.Batch"
RUN dotnet build "TTQ.Batch.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "TTQ.Batch.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "TTQ.Batch.dll"]