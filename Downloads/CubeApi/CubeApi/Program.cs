using Microsoft.AnalysisServices.AdomdClient; // Para conectar con SSAS y trabajar con CellSet
using System.Collections.Generic; // Para usar List y Dictionary
using Microsoft.AspNetCore.Builder; // Para la configuración del WebApplication
using Microsoft.Extensions.DependencyInjection; // Para el servicio del API explorer
using Microsoft.Extensions.Hosting; // Para el manejo del ambiente (Development, Production)

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

// Conexión a SSAS
var ssasConnectionString = "Data Source=LOCALHOST;Catalog=APPSNICA-CUBE;";

// Transformar el resultado en JSON
List<Dictionary<string, object>> TransformToJSON(CellSet result)
{
    var jsonData = new List<Dictionary<string, object>>();
    int cellIndex = 0;

    foreach (var rowPosition in result.Axes[1].Positions)  // Eje de filas (Dimensiones)
    {
        var dataPoint = new Dictionary<string, object>();

        for (int i = 0; i < rowPosition.Members.Count; i++)
        {
            var dimensionName = result.Axes[1].Set.Hierarchies[i].Name;
            dataPoint[dimensionName] = rowPosition.Members[i].Caption; // Añadir nombre de la dimensión y valor
        }

        for (int colIndex = 0; colIndex < result.Axes[0].Positions.Count; colIndex++)
        {
            var measureName = result.Axes[0].Positions[colIndex].Members[0].Caption; // Captura el nombre de la medida
            var cellValue = result.Cells[cellIndex].Value; // Captura el valor correcto de la celda

            dataPoint[measureName] = cellValue;
            cellIndex++; // Aumentar el índice de la celda
        }

        jsonData.Add(dataPoint);
    }

    return jsonData;
}

// GetCubeData endpoint
app.MapGet("/cubedata", () =>
{
    using (AdomdConnection connection = new AdomdConnection(ssasConnectionString))
    {
        connection.Open();
        string query = "SELECT NON EMPTY { [Measures].[Calificacion] } ON COLUMNS, " +
                       "NON EMPTY { HEAD(FILTER( ([Dim Aplicaciones].[Nombre].[Nombre].ALLMEMBERS* [Dim Aplicaciones].[Tipo].[Tipo].ALLMEMBERS  * [Dim Aplicaciones].[Resenas].[Resenas].ALLMEMBERS   * [Dim Aplicaciones].[VersionActual].[VersionActual].ALLMEMBERS), [Dim Aplicaciones].[Tipo].CURRENTMEMBER.MEMBER_CAPTION = 'Gratis' ), 16 ) } " +
                       "DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS " +
                       "FROM [APPSNICADW] " +
                       "CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS";
        using (AdomdCommand command = new AdomdCommand(query, connection))
        {
            var result = command.ExecuteCellSet();
            var jsonResult = TransformToJSON(result);
            return Results.Ok(jsonResult);
        }
    }
})
.WithName("GetCubeData")
.WithOpenApi();


// GetCubeDataWithAppCount endpoint
app.MapGet("/cubedatawithappcount", () =>
{
    using (AdomdConnection connection = new AdomdConnection(ssasConnectionString))
    {
        connection.Open();
        string query = "WITH " +
                       "MEMBER [Measures].[AppCount] AS " +
                       "COUNT(FILTER([Dim Aplicaciones].[Nombre].[Nombre].MEMBERS, NOT ISEMPTY([Measures].[Calificacion]))) " +
                       "MEMBER [Measures].[TotalCalificaciones] AS " +
                       "SUM(FILTER([Dim Aplicaciones].[Nombre].[Nombre].MEMBERS, NOT ISEMPTY([Measures].[Calificacion])), [Measures].[Calificacion]) " +
                       "SELECT { [Measures].[AppCount], [Measures].[TotalCalificaciones] } ON COLUMNS, " +
                       "{ ([Dim Date].[Year].&[2024], [Dim Date].[Month].&[9]) } ON ROWS " +
                       "FROM [APPSNICADW]";
        using (AdomdCommand command = new AdomdCommand(query, connection))
        {
            var result = command.ExecuteCellSet();
            var jsonResult = TransformToJSON(result);
            return Results.Ok(jsonResult);
        }
    }
})
.WithName("GetCubeDataWithAppCount")
.WithOpenApi();


app.MapGet("/getAppRatingAggregates", () =>
{
    string connectionString = "Data Source=LOCALHOST;Catalog=APPSNICA-CUBE;";  // Ajusta la cadena de conexión
    using (AdomdConnection connection = new AdomdConnection(connectionString))
    {
        connection.Open();

        string query = @"
        WITH 
            MEMBER [Measures].[CalificacionesBajas] AS
                COUNT(
                    FILTER(
                        [Dim Aplicaciones].[Nombre].[Nombre].MEMBERS, 
                        [Measures].[Calificacion] < 2.0
                    )
                )
            MEMBER [Measures].[CalificacionesMedias] AS
                COUNT(
                    FILTER(
                        [Dim Aplicaciones].[Nombre].[Nombre].MEMBERS, 
                        [Measures].[Calificacion] >= 2.0 AND [Measures].[Calificacion] < 4.0
                    )
                )
            MEMBER [Measures].[CalificacionesAltas] AS
                COUNT(
                    FILTER(
                        [Dim Aplicaciones].[Nombre].[Nombre].MEMBERS, 
                        [Measures].[Calificacion] >= 4.0
                    )
                )
        SELECT 
            { [Measures].[CalificacionesBajas], [Measures].[CalificacionesMedias], [Measures].[CalificacionesAltas] } ON COLUMNS
        FROM [APPSNICADW]";

        using (AdomdCommand command = new AdomdCommand(query, connection))
        {
            var result = command.ExecuteCellSet();
            var jsonResult = TransformToJSON123(result);
            return Results.Ok(jsonResult);
        }
    }

    // Función para transformar los resultados a JSON
    List<Dictionary<string, object>> TransformToJSON123(CellSet result)
    {
        var jsonData = new List<Dictionary<string, object>>();
        var dataPoint = new Dictionary<string, object>();

        // Asumimos que solo hay medidas en las columnas, sin filas
        for (int colIndex = 0; colIndex < result.Axes[0].Positions.Count; colIndex++)
        {
            var measureName = result.Axes[0].Positions[colIndex].Members[0].Caption;
            var cellValue = result.Cells[colIndex].Value;

            dataPoint[measureName] = cellValue;
        }

        jsonData.Add(dataPoint);

        return jsonData;
    }
})
.WithName("GetAppRatingAggregates")
.WithOpenApi();


app.MapGet("/cubedata/filtroTipoPago", () =>
{
    string connectionString = "Data Source=LOCALHOST;Catalog=APPSNICA-CUBE;";  // Ajusta la cadena de conexión
    using (AdomdConnection connection = new AdomdConnection(connectionString))
    {
        connection.Open();

        // Consulta MDX con filtros específicos
        string myquery = @"
            SELECT 
        NON EMPTY { [Measures].[Calificacion] } ON COLUMNS, 
        NON EMPTY { 
            FILTER(
                [Dim Aplicaciones].[Nombre].[Nombre].ALLMEMBERS * 
                [Dim Aplicaciones].[Tipo].[Tipo].MEMBERS * 
                [Dim Date].[Year].[Year].MEMBERS, 
                [Dim Date].[Year].CURRENTMEMBER.MEMBER_CAPTION = '2024' AND 
                [Dim Aplicaciones].[Tipo].CURRENTMEMBER.MEMBER_CAPTION = 'Pagada' AND 
                NOT ISEMPTY([Measures].[Calificacion]) AND
                [Measures].[Calificacion] >= 4.0 AND
                [Measures].[Calificacion] <= 4.5
        )
    } 
          DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS 
          FROM [APPSNICADW] 
             CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS";

        using (AdomdCommand command = new AdomdCommand(myquery, connection))
        {
            var result = command.ExecuteCellSet();

            // Transformar el resultado en JSON
            var jsonResult = TransformToJSON(result);
            return Results.Ok(jsonResult);
        }
    }


})
.WithName("GetCubeDataWithFiltroPagadas")
.WithOpenApi();




app.MapGet("/cubedata/PromedioCategoria", () =>
{
    string connectionString = "Data Source=LOCALHOST;Catalog=APPSNICA-CUBE;";  // Ajusta la cadena de conexión
    using (AdomdConnection connection = new AdomdConnection(connectionString))
    {
        connection.Open();
        // Consulta MDX con filtros específicos
        string myquery = @"
           WITH 
            MEMBER [Measures].[AvgCalificacion] AS
                AVG(
                    [Dim Aplicaciones].[Nombre].[Nombre].MEMBERS, 
                    [Measures].[Calificacion]
                )
        SELECT 
            NON EMPTY { [Measures].[AvgCalificacion] } ON COLUMNS, 
            NON EMPTY { 
                [Dim Aplicaciones].[NombreCategoria].[NombreCategoria].ALLMEMBERS 
            } 
            DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS 
        FROM [APPSNICADW] 
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS";

        using (AdomdCommand command = new AdomdCommand(myquery, connection))
        {
            var result = command.ExecuteCellSet();

            // Transformar el resultado en JSON
            var jsonResult = TransformToJSON(result);
            return Results.Ok(jsonResult);
        }
    }


})
.WithName("GetCubeDataWithPromedioCategoria")
.WithOpenApi();



//Esta consulta retornal el tipo de aplicacion, el mes, el Quatrimestre y el total de calificacion

app.MapGet("/cubedata/mesquarter", () =>
{
    string connectionString = "Data Source=LOCALHOST;Catalog=APPSNICA-CUBE;";  // Ajusta la cadena de conexión
    using (AdomdConnection connection = new AdomdConnection(connectionString))
    {
        connection.Open();

        // Consulta MDX con filtros específicos
        string myquery = @"
           SELECT 
            NON EMPTY { [Measures].[Calificacion] } ON COLUMNS, 
            NON EMPTY { 
                [Dim Aplicaciones].[Tipo].[Tipo].ALLMEMBERS * 
                [Dim Date].[Quarter].[Quarter].ALLMEMBERS * 
                [Dim Date].[Month Name].[Month Name].ALLMEMBERS 
            } 
            DIMENSION PROPERTIES MEMBER_CAPTION, MEMBER_UNIQUE_NAME ON ROWS 
        FROM [APPSNICADW] 
        CELL PROPERTIES VALUE, BACK_COLOR, FORE_COLOR, FORMATTED_VALUE, FORMAT_STRING, FONT_NAME, FONT_SIZE, FONT_FLAGS";

        using (AdomdCommand command = new AdomdCommand(myquery, connection))
        {
            var result = command.ExecuteCellSet();

            // Transformar el resultado en JSON
            var jsonResult = TransformToJSON(result);
            return Results.Ok(jsonResult);
        }
    }


})
.WithName("GetCubeDataWithMesQuarter")
.WithOpenApi();


app.Run();