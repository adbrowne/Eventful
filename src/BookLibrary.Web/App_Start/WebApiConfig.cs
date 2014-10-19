using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Web.Http;
using System.Web.Http.Dispatcher;
using Autofac.Integration.WebApi;

namespace BookLibrary.Web
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config, AutofacWebApiDependencyResolver resolver)
        {
            // Web API configuration and services
            config.DependencyResolver = resolver;
            config.Services.Replace(typeof(IAssembliesResolver), new MyAssemblyResolver());

            config.EnableSystemDiagnosticsTracing();

            // Web API routes
            config.MapHttpAttributeRoutes();

            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );
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
