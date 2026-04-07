import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { type FormEvent, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { useAuth } from "#/components/auth-provider";
import { Button } from "#/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "#/components/ui/card";
import { Field, FieldContent, FieldError, FieldGroup, FieldLabel } from "#/components/ui/field";
import { Input } from "#/components/ui/input";
import { Spinner } from "#/components/ui/spinner";

export const Route = createFileRoute("/(public)/auth/")({
  component: LoginPage,
});

function LoginPage() {
  const navigate = useNavigate();
  const { signIn, isAuthenticated, isLoading } = useAuth();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [serverError, setServerError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const validationError = useMemo(() => {
    if (!email.trim()) {
      return "Email is required.";
    }
    const normalizedEmail = email.trim();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalizedEmail)) {
      return "Please enter a valid email address.";
    }
    if (!password) {
      return "Password is required.";
    }
    return null;
  }, [email, password]);

  useEffect(() => {
    if (!isLoading && isAuthenticated) {
      void navigate({ to: "/dashboard" });
    }
  }, [isAuthenticated, isLoading, navigate]);

  if (isLoading || isAuthenticated) {
    return (
      <div className={"flex min-h-screen items-center justify-center"}>
        <Spinner className={"size-8"} />
      </div>
    );
  }

  const onSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setServerError(null);

    if (validationError !== null) {
      setServerError(validationError);
      return;
    }

    setIsSubmitting(true);
    try {
      await signIn(email.trim(), password);
      await navigate({ to: "/dashboard" });
    } catch (error) {
      const message = error instanceof Error ? error.message : "Something went wrong.";
      setServerError(message);
      toast.error(message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className={"flex min-h-screen items-center justify-center p-4"}>
      <Card className={"w-full max-w-sm"}>
        <CardHeader>
          <CardTitle className={"text-xl"}>Sign in</CardTitle>
          <CardDescription>Enter your credentials to access your account.</CardDescription>
        </CardHeader>

        <CardContent>
          <form className={"flex flex-col gap-4"} onSubmit={onSubmit}>
            <FieldGroup>
              <Field>
                <FieldLabel htmlFor={"email"}>Email</FieldLabel>
                <FieldContent>
                  <Input
                    autoComplete={"email"}
                    id={"email"}
                    onChange={(event) => setEmail(event.target.value)}
                    placeholder={"you@example.com"}
                    type={"email"}
                    value={email}
                  />
                </FieldContent>
              </Field>

              <Field>
                <FieldLabel htmlFor={"password"}>Password</FieldLabel>
                <FieldContent>
                  <Input
                    autoComplete={"current-password"}
                    id={"password"}
                    onChange={(event) => setPassword(event.target.value)}
                    type={"password"}
                    value={password}
                  />
                </FieldContent>
              </Field>

              {serverError !== null && <FieldError>{serverError}</FieldError>}

              <Button className={"w-full"} disabled={isSubmitting} type={"submit"}>
                {isSubmitting ? <Spinner /> : "Sign in"}
              </Button>
            </FieldGroup>
          </form>
        </CardContent>

        <CardFooter className={"justify-center text-muted-foreground text-sm"}>
          Don't have an account?&nbsp;
          <Link className={"text-foreground underline-offset-4 hover:underline"} to={"/auth/new"}>
            Sign up
          </Link>
        </CardFooter>
      </Card>
    </div>
  );
}
