using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Formatting;
using System.Reflection;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using System.Web.Http.Tracing;
using Autofac.Integration.WebApi;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace BookLibrary.Web
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config, AutofacWebApiDependencyResolver resolver)
        {
            // Web API configuration and services
            config.DependencyResolver = resolver;
            config.Services.Replace(typeof(IAssembliesResolver), new MyAssemblyResolver());
            config.Formatters.JsonFormatter.SerializerSettings.ContractResolver = new EventfulContractResolver();
            var tracer = config.EnableSystemDiagnosticsTracing();
            tracer.MinimumLevel = TraceLevel.Debug;

            // Web API routes
            config.MapHttpAttributeRoutes();

            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );
        }
    }

    public class EventfulContractResolver : DefaultContractResolver
    {
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var baseProperty = base.CreateProperty(member, memberSerialization);
            if (member.GetCustomAttribute<GeneratedIdAttribute>() != null)
            {
                return null;
            }
            else if (member.GetCustomAttribute<FromRouteAttribute>() != null)
            {
                return null;
            }
            else
            {
                return baseProperty;
            }
        }
    }

    // having this here seems to help it find the Controllers in BookLibrary
    // I cannot explain why
    public class MyAssemblyResolver : DefaultAssembliesResolver
    {
        public override ICollection<Assembly> GetAssemblies()
        {
            var assemblies = base.GetAssemblies();
            return assemblies;
        }
    }
}
