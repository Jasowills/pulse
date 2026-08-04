# Pulse.TestApp (MAUI)

The **on-device** counterpart to the headless harness. This project is a **source
scaffold only** — the MAUI workload is intentionally not installed on the dev
machine, so it is **not** in `Pulse.sln` and **does not build** until you install
workloads and the per-platform projects.

Use it (once built) to run the visual / on-device scenarios (`4.4`, row-flash,
mobile lifecycle, status-bar states) that the headless harness cannot.

## What's here

- `MauiProgram.cs` + `App`/`AppShell` — DI + shell routing (`main` → `detail?id=`)
- `MainPage` — list screen: Status/Region filter dropdowns, live rows, a **live
  count badge**, and a bottom **connection-state status bar**
- `DetailPage` — one order subscribed by `_id`, live-updating its status
- `ViewModels/` — `OrderRowViewModel`, `MainViewModel`, `DetailViewModel` (all
  bind directly to `IPulseSubscription<Order>` `OnSnapshot`/`OnChange`)
- `Services/PulseService.cs` — owns the single `PulseClient`, auto-reconnect,
  and raises `ConnectionStateChanged` for the status bar / lifecycle
- `AppConfig.cs` — hub URL + provider label from `PULSE_HUB_URL` / `PULSE_PROVIDER`

## How it maps to the harness

The MAUI screens are thin bindings over the *same* subscription semantics the
harness exercises headlessly (`ListModel` ≈ `Rows` + `CountBadge`). Anything
passing headlessly should render identically on device.

## Finishing the scaffold

```bash
# install the MAUI workload once (large download)
dotnet workload install maui

# generate the per-platform entry points into Platforms/
dotnet new maui -o /tmp/pristine-maui --no-restore
#   then copy /tmp/pristine-maui/Platforms/ into apps/Pulse.TestApp/
```

Then `dotnet build apps/Pulse.TestApp/Pulse.TestApp.csproj`. Add the project to
the solution only if you want CI to build it (it will then require the workload).

## Running against the test server

Start the provider server (see `../Pulse.TestApp.Server`), then set
`PULSE_HUB_URL` (default `http://localhost:5210/pulse`). On a physical device or
emulator use the host machine's LAN IP instead of `localhost`.