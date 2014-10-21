extern alias fsharpxcore;

using System;
using System.Threading.Tasks;
using System.Web.Mvc;
using Eventful;
using Microsoft.FSharp.Core;
using Raven.Client;

namespace BookLibrary.Web.Controllers
{
    public class BooksController : Controller
    {
        private readonly IBookLibrarySystem _system;
        private readonly IAsyncDocumentSession _documentSession;

        public BooksController(IBookLibrarySystem system, IAsyncDocumentSession documentSession)
        {
            _system = system;
            _documentSession = documentSession;
        }

        // GET: Books
        public ActionResult Index()
        {
            return View();
        }

        public ActionResult Create()
        {
            return View();
        }

        public async Task<ActionResult> Edit(Guid id)
        {
            var doc = await _documentSession.LoadAsync<Book.BookDocument>("Book/" + id);
            return View(doc);
        }

        [AcceptVerbs(HttpVerbs.Post)]
        public async Task<ActionResult> Edit(Guid id, UpdateBookTitleCommand cmd)
        {
            cmd.BookId = new BookId(id);

            return await RunCommand(cmd, s => RedirectToAction("Edit"));
        }

        private async Task<ActionResult> RunCommand(UpdateBookTitleCommand cmd, Func<CommandSuccess<BookLibraryEventMetadata>, ActionResult> onSuccess)
        {
            var result = await _system.RunCommandTask(cmd);
            if (result.IsChoice1Of2)
            {
                var successResult =
                    ((
                        FSharpChoice<CommandSuccess<BookLibraryEventMetadata>, fsharpxcore::FSharpx.Collections.NonEmptyList<CommandFailure>>.Choice1Of2)result).Item;
                return onSuccess(successResult);
                
            }
            else
            {
                AddErrorsToModelState(result);
                return View();
            }
        }

        [AcceptVerbs(HttpVerbs.Post)]
        public async Task<ActionResult> Create(AddBookCommand cmd)
        {
            cmd.BookId = new BookId(Guid.NewGuid());

            var result = await _system.RunCommandTask(cmd);
            if (result.IsChoice1Of2)
            {
                var routeValues = new { cmd.BookId.Id };
                return RedirectToAction("Edit", "Books", routeValues);// View("View");
            }
            else
            {
                AddErrorsToModelState(result);
                return View();
            }
        }

        private void AddErrorsToModelState(FSharpChoice<CommandSuccess<BookLibraryEventMetadata>, fsharpxcore::FSharpx.Collections.NonEmptyList<CommandFailure>> result)
        {
            var errorResult =
                ((
                    FSharpChoice<CommandSuccess<BookLibraryEventMetadata>, fsharpxcore::FSharpx.Collections.NonEmptyList<CommandFailure>>.Choice2Of2)result).Item;
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
        }
    }
}