using System;
using System.Linq;
using System.Threading.Tasks;
using System.Web.Mvc;
using Eventful;
using FSharpx.Collections;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;

namespace EmergencyRoom.Web.Controllers
{
    public class VisitController : Controller
    {
        private readonly EmergencyRoomSystem _system;

        public VisitController(EmergencyRoomSystem system)
        {
            _system = system;
        }

        //
        // GET: /Visit/

        public ActionResult Index()
        {
            return View();
        }

        public ActionResult Register()
        {
            return View();
        }

        [AcceptVerbs(HttpVerbs.Post)]
        public async Task<ActionResult> Register(RegisterPatientCommand command)
        {
            command.VisitId = new VisitId(Guid.NewGuid());

            var errors = Visit.validateCommand(command);
            if (errors.Any())
            {
                foreach (var error in errors)
                {
                    var field = OptionModule.IsSome(error.Item1) ? error.Item1.Value : "";
                    var errorMessage = error.Item2;
                    ModelState.AddModelError(field, errorMessage);
                }
                return View();
            }
            else
            {
                FSharpChoice<FSharpList<Tuple<string, object, EmergencyEventMetadata>>, NonEmptyList<CommandFailure>> result = await _system.RunCommand(command);
                if (result.IsChoice1Of2)
                {
                    return View("Success");
                }
                else
                {
                    var errorResult = ((FSharpChoice<FSharpList<Tuple<string, object, EmergencyEventMetadata>>, NonEmptyList<CommandFailure>>.Choice2Of2) result).Item;
                    foreach (var error in errorResult)
                    {
                        var commandError = error as CommandFailure.CommandError;
                        if (commandError != null)
                        {
                            ModelState.AddModelError("", commandError.Item);
                        }

                        var commandExn = error as CommandFailure.CommandException;
                        if (commandExn != null)
                        {
                            ModelState.AddModelError("", commandExn.Item1 + ": " + commandExn.Item2);
                        }
                        var fieldError = error as CommandFailure.CommandFieldError;
                        if (fieldError != null)
                        {
                            ModelState.AddModelError(fieldError.Item1, fieldError.Item2);
                        }
                    }
                    return View();
                }
            }
        }
    }
}