import { redirect } from "next/navigation";

// Root → redirect straight to the live dashboard
export default function Home() {
  redirect("/dashboard");
}
