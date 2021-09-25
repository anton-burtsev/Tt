FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["TTQ/TTQ.csproj", "TTQ/"]
RUN dotnet restore "TTQ/TTQ.csproj"
COPY . .
WORKDIR "/src/TTQ"
RUN dotnet build "TTQ.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "TTQ.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "TTQ.dll"]