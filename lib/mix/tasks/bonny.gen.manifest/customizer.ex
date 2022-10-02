defmodule Mix.Tasks.Bonny.Gen.Manifest.Customizer do
  @moduledoc """
  Overrides manifests generated by `mix bonny.gen.manifest`
  """

  @spec override(Bonny.Resource.t()) :: Bonny.Resource.t()
  def override(%{kind: "ServiceAccount"} = resource) do
    put_in(resource, ~w(metadata labels test)a, "bonny")
  end

  # fallback
  def override(resource), do: resource
end
