FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["TTQ.Load/TTQ.Load.csproj", "TTQ.Load/"]
RUN dotnet restore "TTQ.Load/TTQ.Load.csproj"
COPY . .
WORKDIR "/src/TTQ.Load"
RUN dotnet build "TTQ.Load.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "TTQ.Load.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "TTQ.Load.dll"]