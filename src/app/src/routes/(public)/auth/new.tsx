import { createFileRoute, Link, useNavigate } from "@tanstack/react-router";
import { type FormEvent, useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { useAuth } from "#/components/auth-provider";
import { Button } from "#/components/ui/button";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "#/components/ui/card";
import { Field, FieldContent, FieldError, FieldGroup, FieldLabel } from "#/components/ui/field";
import { Input } from "#/components/ui/input";
import { Spinner } from "#/components/ui/spinner";

export const Route = createFileRoute("/(public)/auth/new")({
  component: RegisterPage,
});

function RegisterPage() {
  const navigate = useNavigate();
  const { signUp, isAuthenticated, isLoading } = useAuth();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
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
    if (password.length < 8) {
      return "Password must be at least 8 characters.";
    }
    if (password !== confirmPassword) {
      return "Passwords do not match.";
    }
    return null;
  }, [confirmPassword, email, password]);

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
      await signUp(email.trim(), password);
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
          <CardTitle className={"text-xl"}>Create an account</CardTitle>
          <CardDescription>Enter your details below to get started.</CardDescription>
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
                    autoComplete={"new-password"}
                    id={"password"}
                    onChange={(event) => setPassword(event.target.value)}
                    type={"password"}
                    value={password}
                  />
                </FieldContent>
              </Field>

              <Field>
                <FieldLabel htmlFor={"confirm-password"}>Confirm password</FieldLabel>
                <FieldContent>
                  <Input
                    autoComplete={"new-password"}
                    id={"confirm-password"}
                    onChange={(event) => setConfirmPassword(event.target.value)}
                    type={"password"}
                    value={confirmPassword}
                  />
                </FieldContent>
              </Field>

              {serverError !== null && <FieldError>{serverError}</FieldError>}

              <Button className={"w-full"} disabled={isSubmitting} type={"submit"}>
                {isSubmitting ? <Spinner /> : "Create account"}
              </Button>
            </FieldGroup>
          </form>
        </CardContent>

        <CardFooter className={"justify-center text-muted-foreground text-sm"}>
          Already have an account?&nbsp;
          <Link className={"text-foreground underline-offset-4 hover:underline"} to={"/auth"}>
            Sign in
          </Link>
        </CardFooter>
      </Card>
    </div>
  );
}
