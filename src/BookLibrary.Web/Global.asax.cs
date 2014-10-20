using System;
using System.Web.Http;
using System.Web.Mvc;
using System.Web.Optimization;
using System.Web.Routing;
using Autofac;
using Autofac.Integration.Mvc;
using Autofac.Integration.WebApi;
using BookLibrary.Web.Controllers;
using Raven.Client;

namespace BookLibrary.Web
{
    public class WebApiApplication : System.Web.HttpApplication
    {
        protected void Application_Start()
        {
            // Create the container builder.
            var builder = new ContainerBuilder();

            // Register the Web API controllers.
            builder.RegisterApiControllers(typeof(BooksController).Assembly);
            builder.RegisterControllers(typeof(WebApiApplication).Assembly);
            RegisterEventStore(builder);
            RegisterRaven(builder);
            // Build the container.
            var container = builder.Build();

            // Create the depenedency resolver.
            var resolver = new AutofacWebApiDependencyResolver(container);

            // Configure Web API with the dependency resolver.
            GlobalConfiguration.Configuration.DependencyResolver = resolver;

            AreaRegistration.RegisterAllAreas();
            GlobalConfiguration.Configure(config => WebApiConfig.Register(config, resolver));
            FilterConfig.RegisterGlobalFilters(GlobalFilters.Filters);
            RouteConfig.RegisterRoutes(RouteTable.Routes);
            BundleConfig.RegisterBundles(BundleTable.Bundles);

            // mvc autofac config
            // DependencyResolver.SetResolver(new AutofacDependencyResolver(container));
            DependencyResolver.SetResolver(new AutofacDependencyResolver(container));

        }

        private void RegisterRaven(ContainerBuilder builder)
        {
            builder.Register(c => ApplicationConfig.buildDocumentStore())
                .AsImplementedInterfaces()
                .SingleInstance();

            builder.Register(c =>
            {
                var documentStore = c.Resolve<IDocumentStore>();
                return documentStore.OpenAsyncSession();
            }).InstancePerRequest();
        }

        private static void RegisterEventStore(ContainerBuilder builder)
        {
            var systemTask = ApplicationConfig.initializedSystem();
            systemTask.Wait();
            var system = systemTask.Result;
            builder.RegisterInstance(system).SingleInstance();
        }
    }
}
